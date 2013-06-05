{-# LANGUAGE OverloadedStrings, StandaloneDeriving, DeriveDataTypeable #-}
module Database.PropertyGraph.Neo4jBatch (
    runPropertyGraphT) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,Properties,Label,
    VertexId(VertexId),
    PropertyGraphF(NewVertex,NewEdge))

import Control.Proxy (Proxy,Producer,Pipe,Consumer,Server,request,respond,lift,(>->),(>~>),runProxyK,toListD,mapD,unitU,foreverK)
import Control.Proxy.Safe (ExceptionP,SafeIO,runEitherK,throw)
import Control.Proxy.Parse (returnPull,unwrap,draw,drawAll,passUpTo,wrap)
import qualified Control.Proxy.Safe as Proxy (tryIO)
import Control.Proxy.Trans (liftP)
import Control.Proxy.Morph (hoistP)
import Control.Proxy.Trans.State (StateP,evalStateK,modify,get,evalStateP)
import Control.Proxy.Trans.Writer (execWriterK)

import Control.Monad (forever,mzero,replicateM,(>=>))
import Control.Applicative ((<$>))
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Exception (IOException,SomeException,toException,Exception,ErrorCall(ErrorCall))

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
import Data.Data (Data)
import Data.Typeable (Typeable)
import Data.List (stripPrefix)

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
                       ConnectionClosingError IOException deriving (Typeable)

data AnyId = Temporary TemporaryId | Permanent PermanentUri deriving (Show,Eq,Ord)
data TemporaryId = TemporaryId Integer deriving (Show,Eq,Ord)
data PermanentUri = PermanentUri Text deriving (Show,Eq,Ord)

data Neo4jBatchRequest = VertexRequest Properties TemporaryId
                       | EdgeRequest Properties Label AnyId AnyId Integer deriving (Show)

data Neo4jBatchResponses = Neo4jBatchResponses (Vector Neo4jBatchResponse) deriving (Show)
data Neo4jBatchResponse = Neo4jBatchResponse (Maybe Integer) Text (Maybe Text) (Maybe JSON.Value) deriving (Show)

runPropertyGraphT :: Hostname -> Port -> PropertyGraphT SafeIO r -> SafeIO (Either SomeException r)
runPropertyGraphT hostname port propertygraph = runProxyK (runEitherK (
    wrap . (evalStateK 0 (interpretPropertyGraphT propertygraph)) >->
    evalStateK [] (chunk 1000) >->
    evalStateK Map.empty replace >->
    (extract >~>
    consume "localhost" 7474))) []

consume :: (Proxy p) => Hostname -> Port -> [Neo4jBatchRequest] -> (ExceptionP p) Neo4jBatchResponses [Neo4jBatchRequest] x' x SafeIO r
consume hostname port neo4jbatchrequests = do
    result <- Proxy.tryIO (add hostname port neo4jbatchrequests)
    neo4jbatchresponses <- either (throw.toException) return result
    request neo4jbatchresponses >>= consume hostname port

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

            jsonvalue <- tryIO (receiveResponse connection (\response responsebody -> (do
                parseFromStream JSON.json responsebody)))
                `onFailure` ResponseReceivalError

            responses <- case fromJSON jsonvalue of
                Error e        -> left (ResponseParseError e)
                Success result -> return result

            tryIO (closeConnection connection)
                `onFailure` ConnectionClosingError

            return responses)

deriving instance Show Neo4jBatchError
instance Exception Neo4jBatchError

onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT

instance ToJSON Neo4jBatchRequest where
    toJSON (VertexRequest properties (TemporaryId vertexid)) = JSON.object [
        "method" .= ("POST" :: Text),
        "to"     .= ("/node" :: Text),
        "id"     .= toJSON vertexid,
        "body"   .= toJSON properties]
    toJSON (EdgeRequest properties label sourceid targetid runningid) = JSON.object [
        "method" .= ("POST" :: Text),
        "to"     .= (vertexidURI sourceid `append` "/relationships"),
        "id"     .= toJSON runningid,
        "body"   .= JSON.object [
            "to"   .= vertexidURI targetid,
            "type" .= label,
            "data" .= properties]]

instance FromJSON Neo4jBatchResponses where
    parseJSON (JSON.Array array) = do
        responses <- Vector.mapM parseJSON array
        return (Neo4jBatchResponses responses)
    parseJSON _             = fail "responses are no array"

instance FromJSON Neo4jBatchResponse where
    parseJSON (JSON.Object object) = do
        temporaryid <- object .:? "id"
        from        <- object .: "from"
        location    <- object .:? "location"
        body        <- object .:? "body"
        return (Neo4jBatchResponse temporaryid from location body)
    parseJSON _ = fail "response is not an object"

vertexidURI :: AnyId -> Text
vertexidURI (Temporary (TemporaryId vertexid)) = pack ("{" ++ show vertexid ++ "}")
vertexidURI (Permanent (PermanentUri vertexuri)) = vertexuri

extract :: (Proxy p,Monad m) => x -> (ExceptionP p) (Map TemporaryId PermanentUri) x Neo4jBatchResponses x m r
extract x = do
    neo4jbatchresponses <- respond x
    let result = matchTemporaryIds neo4jbatchresponses
    temporaryidmap <- either (throw . ErrorCall) return result
    request temporaryidmap >>= extract

matchTemporaryIds :: Neo4jBatchResponses -> Either String (Map TemporaryId PermanentUri)
matchTemporaryIds (Neo4jBatchResponses vector) = do
    pairs <- Vector.mapM responseToParing vector
    return (Map.fromList (concat (Vector.toList pairs)))

responseToParing :: Neo4jBatchResponse -> Either String [(TemporaryId,PermanentUri)]
responseToParing (Neo4jBatchResponse (Just temporaryid) _ (Just completeuri) _) = do
    case parseURI (unpack completeuri) of
        Nothing -> Left "couldn't parse uri"
        Just uri -> do
            case stripPrefix "/db/data" (uriPath uri) of
                Just permanenturi -> return [(TemporaryId temporaryid,PermanentUri (pack permanenturi))]
                Nothing -> Left "uri path doesn't start with /db/data"
responseToParing _ = Right []

replace :: (Proxy p,Monad m) =>
    x -> (StateP (Map TemporaryId PermanentUri) p) x [Neo4jBatchRequest] (Map TemporaryId PermanentUri) [Neo4jBatchRequest] m r
replace x = do
    neo4jbatchrequests <- request x
    temporaryidmap <- get
    let neo4jbatchrequests' = map (replaceId temporaryidmap) neo4jbatchrequests
    temporaryidmap' <- respond neo4jbatchrequests'
    modify (Map.union temporaryidmap')
    replace x

replaceId :: (Map TemporaryId PermanentUri) -> Neo4jBatchRequest -> Neo4jBatchRequest
replaceId permanentids (EdgeRequest properties label sourceid targetid runningid) =
    EdgeRequest properties label (replace sourceid) (replace targetid) runningid where
        replace (Temporary temporaryid) =
            maybe (Temporary temporaryid) Permanent
                (Map.lookup temporaryid permanentids)
        replace permanentid = permanentid
replaceId _ vertexrequest = vertexrequest

chunk :: (Proxy p,Monad m) => Int -> x -> (StateP [a] p) () (Maybe a) b' [a] m r
chunk n _ = forever (((passUpTo n >-> drawAll) >=> respond) ())

interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> x -> (StateP Integer p) a' a b' Neo4jBatchRequest m r
interpretPropertyGraphT propertygraph x = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do

            temporaryid <- modify (+1) >> get
            respond (VertexRequest properties (TemporaryId temporaryid))
            interpretPropertyGraphT (continue (VertexId temporaryid)) x

        Free (NewEdge properties label (VertexId sourceid) (VertexId targetid) continue) -> do

            runningid <- modify (+1) >> get
            respond (EdgeRequest properties label (Temporary (TemporaryId sourceid)) (Temporary (TemporaryId targetid)) runningid)
            interpretPropertyGraphT continue x

