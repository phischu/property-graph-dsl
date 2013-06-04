{-# LANGUAGE OverloadedStrings, StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch () where

import Database.PropertyGraph.Internal (
    PropertyGraphT,Properties,Label,
    VertexId(Temporary,Permanent),
    TemporaryId(TemporaryId),PermanentUri(PermanentUri),
    PropertyGraphF(NewVertex,NewEdge))

import Control.Proxy (Proxy,Producer,Consumer,request,respond,lift)
import Control.Proxy.Trans (liftP)
import Control.Proxy.Trans.State (StateP,evalStateP,modify,get)

import Control.Monad (forever,mzero)
import Control.Applicative ((<$>))
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Exception (IOException)

import Control.Error (EitherT,fmapLT,runEitherT,tryIO)

import Data.Aeson (
    ToJSON,toJSON,(.=),
    FromJSON(parseJSON),fromJSON,(.:),(.:?),Result(Error,Success))
import qualified Data.Aeson as JSON (
    Value(Array,Object),object,encode,json)
import Data.Text (Text,append,pack,unpack)
import Data.Map (Map)
import Data.Vector (Vector)
import qualified Data.Vector as Vector (mapM,toList,concat,singleton,empty)
import qualified Data.Map as Map (union,fromList)

import Network.Http.Client (
    Hostname,Port,openConnection,closeConnection,
    buildRequest,http,Method(POST),setAccept,setContentType,
    sendRequest,inputStreamBody,
    receiveResponse)
import qualified Data.ByteString.Char8 as BS (pack)

import System.IO.Streams.ByteString (fromLazyByteString)
import System.IO.Streams.Attoparsec (parseFromStream)

import Network.URI (parseURI,uriPath)

{-
import Control.Proxy (Producer)

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraph,
    PropertyGraphF(NewVertex,NewEdge),VertexId(TemporaryId),
    Properties,Label)

import qualified Data.Aeson as JSON (Value(Array),encode,toJSON,json)

import Control.Monad.Writer.Lazy (WriterT,runWriterT,tell)
import Control.Monad.State.Lazy (StateT,evalStateT,modify,get,lift)
import Control.Monad.Identity (Identity,runIdentity)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Error (EitherT,runEitherT,tryIO,fmapLT)
import Control.Exception (IOException)

import qualified Data.Map as Map (fromList)
import Data.Monoid ((<>))
import qualified Data.Vector as Vector (fromList)
import Data.Text (Text,pack)



-- | Convert a given property graph to the equivalent body of a neo4j batch request.
convertPropertyGraphToNeo4jBatch :: PropertyGraph a -> JSON.Value
convertPropertyGraphToNeo4jBatch = snd . runIdentity . runPropertyGraphT

-- | Run a property graph transformer and yield the corresponding property graph
--   as a batch request suitable for neo4j as the second part of the result.
runPropertyGraphT :: (Monad m,Functor m) => PropertyGraphT m a -> m (a,JSON.Value)
runPropertyGraphT =
    fmap (fmap (JSON.Array . Vector.fromList)) .
    flip evalStateT 0 .
    runWriterT .
    interpretPropertyGraph

-- | Interpret a property graph as the list of neo4j requests and a running unique Id.
interpretPropertyGraph :: (Monad m) => PropertyGraphT m a -> WriterT [JSON.Value] (StateT Integer m) a
interpretPropertyGraph propertygraph = do

    next <- lift (lift (runFreeT propertygraph))

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            VertexId vertexid <- lift (modify (+1) >> get) >>= return . VertexId
            tell [vertexRequest properties vertexid]
            interpretPropertyGraph (continue (VertexId vertexid))

        Free (NewEdge properties label (VertexId from) (VertexId to) continue) -> do
            runningid <- lift (modify (+1) >> get) >>= return
            let tempid x = pack ("{" ++ show x ++ "}")
            tell [edgeRequest properties label (tempid from) (tempid to) runningid]
            interpretPropertyGraph continue






-}


-- | Things that can go wrong when submitting a property graph to a neo4j database.
data Neo4jBatchError = OpeningConnectionError IOException |
                       RequestBuildingError IOException |
                       EncodingError IOException |
                       RequestSendingError IOException |
                       ResponseReceivalError IOException |
                       ConnectionClosingError IOException


data Neo4jBatchRequest = VertexRequest Properties TemporaryId
                       | EdgeRequest Properties Label VertexId VertexId

data Neo4jBatchResponses = Neo4jBatchResponses (Vector Neo4jBatchResponse)
data Neo4jBatchResponse = Neo4jBatchResponse Integer Text (Maybe Text) (Maybe JSON.Value)

runPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> Producer p Neo4jBatchRequest m r
runPropertyGraphT propertygraph = evalStateP 0 (interpretPropertyGraphT propertygraph)

interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> Producer (StateP Integer p) Neo4jBatchRequest m r
interpretPropertyGraphT propertygraph = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do

            temporaryid <- modify (+1) >> get >>= return . TemporaryId
            respond (VertexRequest properties temporaryid)
            interpretPropertyGraphT (continue temporaryid)

        Free (NewEdge properties label sourceid targetid continue) -> do

            modify (+1)
            respond (EdgeRequest properties label sourceid targetid)
            interpretPropertyGraphT continue

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
    Hostname -> Port -> () -> Consumer (StateP (Map TemporaryId PermanentUri) p) [JSON.Value] m r
insertPropertyGraphT hostname port () = forever (do
    requestvalues <- request ()
    result <- liftIO (add hostname port (toJSON requestvalues))
    case result of
        Left e -> liftIO (print e)
        Right resultvalue -> do
            case matchTemporaryIds resultvalue of
                Left e -> liftIO (print e)
                Right vertexidmap -> modify (Map.union vertexidmap))

matchTemporaryIds :: JSON.Value -> Either String (Map TemporaryId PermanentUri)
matchTemporaryIds batchresponsevalue = do
    case fromJSON batchresponsevalue of
        Error e -> Left e
        Success batchresponse -> case batchresponse of
            Neo4jBatchResponses vector -> do
                pairs <- Vector.mapM responseToParing vector
                return (Map.fromList (concat (Vector.toList pairs)))
            _ -> Left "batch response not a vector"

responseToParing :: Neo4jBatchResponse -> Either String [(TemporaryId,PermanentUri)]
responseToParing (Neo4jBatchResponse temporaryid _ (Just completeuri) _) = do
    case parseURI (unpack completeuri) of
        Nothing -> Left "couldn't parse uri"
        Just uri -> return [(TemporaryId temporaryid,PermanentUri (pack (uriPath uri)))]
responseToParing _ = Right []

-- | Issue a JSON value representing a neo4j batch request to a neo4j database.
--   Return the decoded response.
add :: Hostname -> Port -> JSON.Value -> IO (Either Neo4jBatchError JSON.Value)
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

            tryIO (closeConnection connection)
                `onFailure` ConnectionClosingError

            return jsonvalue)

deriving instance Show Neo4jBatchError

onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT
