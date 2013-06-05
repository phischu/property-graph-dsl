{-# LANGUAGE OverloadedStrings, StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch (
    runPropertyGraphT) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,Properties,Label,
    VertexId(Temporary,Permanent),
    TemporaryId(TemporaryId),PermanentUri(PermanentUri),
    PropertyGraphF(NewVertex,NewEdge))

import Control.Proxy (Proxy,Producer,Pipe,Consumer,request,respond,lift,(>->),runProxy)
import Control.Proxy.Trans (liftP)
import Control.Proxy.Morph (hoistP)
import Control.Proxy.Trans.State (StateP,evalStateK,modify,get)

import Control.Monad (forever,mzero,(>=>),replicateM)
import Control.Applicative ((<$>))
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Exception (IOException)

import Control.Error (EitherT,fmapLT,runEitherT,tryIO,left,fmapL)

import Data.Aeson (
    ToJSON,toJSON,(.=),
    FromJSON(parseJSON),fromJSON,(.:),(.:?),Result(Error,Success))
import qualified Data.Aeson as JSON (
    Value(Array,Object),object,encode,json)
import Data.Text (Text,append,pack,unpack)
import Data.Map (Map)
import Data.Vector (Vector)
import qualified Data.Vector as Vector (mapM,toList,concat,singleton,empty)
import qualified Data.Map as Map (union,fromList,lookup,empty)

import Network.Http.Client (
    Hostname,Port,openConnection,closeConnection,
    buildRequest,http,Method(POST),setAccept,setContentType,
    sendRequest,inputStreamBody,
    receiveResponse)
import qualified Data.ByteString.Char8 as BS (pack)

import System.IO.Streams.ByteString (fromLazyByteString)
import System.IO.Streams.Attoparsec (parseFromStream)

import Network.URI (parseURI,uriPath)

-- | Things that can go wrong when submitting a property graph to a neo4j database.
data Neo4jBatchError = OpeningConnectionError IOException |
                       RequestBuildingError IOException |
                       EncodingError IOException |
                       RequestSendingError IOException |
                       ResponseReceivalError IOException |
                       ResponseParseError String |
                       ConnectionClosingError IOException


data Neo4jBatchRequest = VertexRequest Properties TemporaryId
                       | EdgeRequest Properties Label VertexId VertexId

data Neo4jBatchResponses = Neo4jBatchResponses (Vector Neo4jBatchResponse)
data Neo4jBatchResponse = Neo4jBatchResponse Integer Text (Maybe Text) (Maybe JSON.Value)

runPropertyGraphT :: (MonadIO m) => Hostname -> Port -> Int -> PropertyGraphT m r -> m r
runPropertyGraphT hostname port n propertygraph = runProxy (evalStateK 0 (evalStateK Map.empty (
    (liftP . (interpretPropertyGraphT propertygraph)) >->
    chunk n >->
    ((hoistP liftP) .) (insertPropertyGraphT hostname port))))

chunk :: (Proxy p,Monad m,Monad (Pipe p a [a] m)) => Int -> () -> Pipe p a [a] m r
chunk n () = forever (do
    xs <- replicateM n (request ())
    respond xs)

interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> () -> Producer (StateP Integer p) Neo4jBatchRequest m r
interpretPropertyGraphT propertygraph () = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do

            temporaryid <- modify (+1) >> get >>= return . TemporaryId
            respond (VertexRequest properties temporaryid)
            interpretPropertyGraphT (continue temporaryid) ()

        Free (NewEdge properties label sourceid targetid continue) -> do

            modify (+1)
            respond (EdgeRequest properties label sourceid targetid)
            interpretPropertyGraphT continue ()

instance ToJSON Neo4jBatchRequest where
    toJSON (VertexRequest properties (TemporaryId vertexid)) = JSON.object [
        "method" .= ("POST" :: Text),
        "to"     .= ("/node" :: Text),
        "id"     .= toJSON vertexid,
        "body"   .= toJSON properties]
    toJSON (EdgeRequest properties label sourceid targetid) = JSON.object [
        "method" .= ("POST" :: Text),
        "to"     .= (vertexidURI sourceid `append` "/relationships"),
        "body"   .= JSON.object [
            "to"   .= vertexidURI targetid,
            "type" .= label,
            "data" .= properties]]

instance FromJSON Neo4jBatchResponses where
    parseJSON (JSON.Array array) = do
        responses <- Vector.mapM parseJSON array
        return (Neo4jBatchResponses responses)
    parseJSON _             = mzero

instance FromJSON Neo4jBatchResponse where
    parseJSON (JSON.Object object) = do
        temporaryid <- object .: "id"
        from        <- object .: "from"
        location    <- object .:? "location"
        body        <- object .:? "body"
        return (Neo4jBatchResponse temporaryid from location body)
    parseJSON _ = mzero

vertexidURI :: VertexId -> Text
vertexidURI (Temporary (TemporaryId vertexid)) = pack ("{" ++ show vertexid ++ "}")
vertexidURI (Permanent (PermanentUri vertexuri)) = vertexuri

-- | Insert chunks of batch requests into a neo4j database...
insertPropertyGraphT :: (Proxy p,MonadIO m) =>
    Hostname -> Port -> () -> Consumer (StateP (Map TemporaryId PermanentUri) p) [Neo4jBatchRequest] m r
insertPropertyGraphT hostname port () = forever (do

    requestvalues <- request ()

    permanentids <- get

    result <- liftIO (add hostname port (replaceIds permanentids requestvalues))

    case result of
        Left e -> liftIO (print e)
        Right resultvalue -> do
            case matchTemporaryIds resultvalue of
                Left e -> liftIO (print e)
                Right newpermanentids -> modify (Map.union newpermanentids))

replaceIds :: (Map TemporaryId PermanentUri) -> [Neo4jBatchRequest] -> [Neo4jBatchRequest]
replaceIds permanentids = map (replaceId permanentids)

replaceId :: (Map TemporaryId PermanentUri) -> Neo4jBatchRequest -> Neo4jBatchRequest
replaceId permanentids (EdgeRequest properties label sourceid targetid) =
    EdgeRequest properties label (replace sourceid) (replace targetid) where
        replace (Temporary temporaryid) =
            maybe (Temporary temporaryid) Permanent
                (Map.lookup temporaryid permanentids)

matchTemporaryIds :: Neo4jBatchResponses -> Either String (Map TemporaryId PermanentUri)
matchTemporaryIds (Neo4jBatchResponses vector) = do
    pairs <- Vector.mapM responseToParing vector
    return (Map.fromList (concat (Vector.toList pairs)))

responseToParing :: Neo4jBatchResponse -> Either String [(TemporaryId,PermanentUri)]
responseToParing (Neo4jBatchResponse temporaryid _ (Just completeuri) _) = do
    case parseURI (unpack completeuri) of
        Nothing -> Left "couldn't parse uri"
        Just uri -> return [(TemporaryId temporaryid,PermanentUri (pack (uriPath uri)))]
responseToParing _ = Right []

-- | Issue a JSON value representing a neo4j batch request to a neo4j database.
--   Return the decoded response.
add :: Hostname -> Port -> [Neo4jBatchRequest] -> IO (Either Neo4jBatchError Neo4jBatchResponses)
add hostname port neo4jbatch = runEitherT (do

            connection <- tryIO (openConnection hostname port)
                `onFailure` OpeningConnectionError

            request <- tryIO (buildRequest (do
                http POST (BS.pack "/db/data/batch")
                setAccept (BS.pack "application/json")
                setContentType (BS.pack "application/json")
                )) `onFailure` RequestBuildingError

            bodyInputStream <- tryIO (fromLazyByteString (JSON.encode (neo4jbatch)))
                `onFailure` EncodingError

            tryIO (sendRequest connection request (inputStreamBody bodyInputStream))
                `onFailure` RequestSendingError

            jsonvalue <- tryIO (receiveResponse connection (\_ responsebody -> (do
                parseFromStream JSON.json responsebody)))
                `onFailure` ResponseReceivalError

            responses <- case fromJSON jsonvalue of
                Error e        -> left (ResponseParseError e)
                Success result -> return result

            tryIO (closeConnection connection)
                `onFailure` ConnectionClosingError

            return responses)

deriving instance Show Neo4jBatchError

onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT
