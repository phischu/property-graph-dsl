{-# LANGUAGE OverloadedStrings, StandaloneDeriving, DeriveDataTypeable #-}
module Database.PropertyGraph.Neo4jBatch (
    runPropertyGraphT,
    Neo4jBatchError(..)) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,Properties,Label,
    VertexId(VertexId),
    PropertyGraphF(NewVertex,NewEdge))

import Control.Proxy (Proxy,request,respond,lift,(>->),(>~>),runProxyK)
import Control.Proxy.Parse (drawAll,passUpTo,wrap)
import Control.Proxy.Trans.State (StateP,evalStateK,modify,get)
import Control.Proxy.Trans.Either (EitherP,runEitherK,throw)

import Control.Monad (forever,(>=>))
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Exception (IOException)

import Control.Error (EitherT,fmapLT,runEitherT,tryIO,left)

import Data.Aeson (
    ToJSON,toJSON,(.=),
    FromJSON(parseJSON),fromJSON,(.:),(.:?),Result(Error,Success))
import qualified Data.Aeson as JSON (
    Value(Array,Object),object,encode,json)
import Data.Text (Text,append,pack,unpack)
import Data.Map (Map)
import Data.Vector (Vector)
import qualified Data.Vector as Vector (mapM,toList)
import qualified Data.Map as Map (union,fromList,lookup,empty)
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

-- | Submit the given property graph to the given host and port in chunks of the given size.
--   Returns errors as 'Left's.
runPropertyGraphT :: (MonadIO m) => Hostname -> Port -> Int -> PropertyGraphT m r -> m (Either Neo4jBatchError r)
runPropertyGraphT hostname port n propertygraph = runProxyK (runEitherK (
    wrap . (evalStateK 0 (interpretPropertyGraphT propertygraph)) >->
    evalStateK [] (chunk n) >->
    evalStateK Map.empty replace >->
    (extract >~>
    consume hostname port))) []

-- | Things that can go wrong when submitting a batch request to a neo4j database.
data Neo4jBatchError = OpeningConnectionError IOException |
                       RequestBuildingError IOException |
                       EncodingError IOException |
                       RequestSendingError IOException |
                       ResponseReceivalError IOException |
                       ResponseParseError String |
                       ConnectionClosingError IOException |
                       IdMapExtractionError String

deriving instance Show Neo4jBatchError

-- | Interpret a property graph into a producer of parts of a neo4j batch request. All ids are
--   temporary.
interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> x -> (StateP Integer p) a' a b' Neo4jBatchRequest m r
interpretPropertyGraphT propertygraph x = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure result -> return result

        Free (NewVertex properties continue) -> do

            temporaryid <- modify (+1) >> get
            _ <- respond (VertexRequest properties (TemporaryId temporaryid))
            interpretPropertyGraphT (continue (VertexId temporaryid)) x

        Free (NewEdge properties label (VertexId sourceid) (VertexId targetid) continue) -> do

            runningid <- modify (+1) >> get
            _ <- respond (EdgeRequest
                properties
                label
                (Temporary (TemporaryId sourceid))
                (Temporary (TemporaryId targetid))
                runningid)
            interpretPropertyGraphT continue x

-- | A chunking proxy. The argument is the size of the chunks.
chunk :: (Proxy p,Monad m) => Int -> x -> (StateP [a] p) () (Maybe a) b' [a] m r
chunk n _ = forever (((passUpTo n >-> drawAll) >=> respond) ())

-- | Keeps a map from temporary ids to permanent uris and replaces in all requests flowing
--   downstream any known temporary id by the permanent uri. Also updates the map.
replace :: (Proxy p,Monad m) =>
    x -> (StateP (Map TemporaryId PermanentUri) p) x [Neo4jBatchRequest] (Map TemporaryId PermanentUri) [Neo4jBatchRequest] m r
replace x = do
    neo4jbatchrequests <- request x
    temporaryidmap <- get
    let neo4jbatchrequests' = map (replaceId temporaryidmap) neo4jbatchrequests
    temporaryidmap' <- respond neo4jbatchrequests'
    modify (Map.union temporaryidmap')
    replace x

-- | Replaces all temporary ids in a single request by their matching permanent uri
--   if the temporary ids are found in the given map.
replaceId :: (Map TemporaryId PermanentUri) -> Neo4jBatchRequest -> Neo4jBatchRequest
replaceId permanentids (EdgeRequest properties label sourceid targetid runningid) =
    EdgeRequest properties label (maybeReplace sourceid) (maybeReplace targetid) runningid where
        maybeReplace (Temporary temporaryid) =
            maybe (Temporary temporaryid) Permanent
                (Map.lookup temporaryid permanentids)
        maybeReplace permanentid = permanentid
replaceId _ vertexrequest = vertexrequest

-- | Extracts a map from temporary id to permanent uri from the neo4j batch responses flowing
--   upstream.
extract :: (Proxy p,Monad m) => x -> (EitherP Neo4jBatchError p) (Map TemporaryId PermanentUri) x Neo4jBatchResponses x m r
extract x = do
    neo4jbatchresponses <- respond x
    let result = matchTemporaryIds neo4jbatchresponses
    temporaryidmap <- either (throw . IdMapExtractionError) return result
    request temporaryidmap >>= extract

-- | From the given neo4j batch responses extract a map from temporary ids to permanent
--   uris. Might fail if the extraction for a single response fails.
matchTemporaryIds :: Neo4jBatchResponses -> Either String (Map TemporaryId PermanentUri)
matchTemporaryIds (Neo4jBatchResponses vector) = do
    pairs <- Vector.mapM responseToParing vector
    return (Map.fromList (concat (Vector.toList pairs)))

-- | From a single response try to find a match between a temporary id and a permanent
--   uri. The temporary id is from the id field and the permanent uri is part of the
--   location field.
responseToParing :: Neo4jBatchResponse -> Either String [(TemporaryId,PermanentUri)]
responseToParing (Neo4jBatchResponse (Just temporaryid) _ (Just completeuri) _) = do
    case parseURI (unpack completeuri) of
        Nothing -> Left "couldn't parse uri"
        Just uri -> do
            case stripPrefix "/db/data" (uriPath uri) of
                Just permanenturi -> return [(TemporaryId temporaryid,PermanentUri (pack permanenturi))]
                Nothing -> Left "uri path doesn't start with /db/data"
responseToParing _ = Right []

-- | A sink for neo4j batch requests doing all network stuff and requesting more
--   requests with the parsed responses.
consume :: (Proxy p,MonadIO m) =>
    Hostname -> Port -> [Neo4jBatchRequest] ->
    (EitherP Neo4jBatchError p) Neo4jBatchResponses [Neo4jBatchRequest] x' x m r
consume hostname port neo4jbatchrequests = do
    result <- lift (liftIO (add hostname port neo4jbatchrequests))
    neo4jbatchresponses <- either throw return result
    request neo4jbatchresponses >>= consume hostname port

-- | Issue a JSON value representing a neo4j batch request to a neo4j database.
--   Return the decoded response.
add :: Hostname -> Port -> [Neo4jBatchRequest] -> IO (Either Neo4jBatchError Neo4jBatchResponses)
add hostname port neo4jbatch = runEitherT (do

            connection <- tryIO (openConnection hostname port)
                `onFailure` OpeningConnectionError

            httprequest <- tryIO (buildRequest (do
                http POST (BS.pack "/db/data/batch")
                setAccept (BS.pack "application/json")
                setContentType (BS.pack "application/json")
                )) `onFailure` RequestBuildingError

            bodyInputStream <- tryIO (fromLazyByteString (JSON.encode (neo4jbatch)))
                `onFailure` EncodingError

            tryIO (sendRequest connection httprequest (inputStreamBody bodyInputStream))
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

-- | Annotate an error.
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

-- | Convert either a temporary id or a permanent uri to its neo4j json representation.
vertexidURI :: AnyId -> Text
vertexidURI (Temporary (TemporaryId vertexid)) = pack ("{" ++ show vertexid ++ "}")
vertexidURI (Permanent (PermanentUri vertexuri)) = vertexuri

-- | Identification for a Node. Either a temporary id if it is part of a batch
--   request or a node uri if it is already in the database.
data AnyId = Temporary TemporaryId | Permanent PermanentUri deriving (Show,Eq,Ord)

-- | A temporary id of a node that is part of a batch request.
data TemporaryId = TemporaryId Integer deriving (Show,Eq,Ord)

-- | The uri of a node in a neo4j database.
data PermanentUri = PermanentUri Text deriving (Show,Eq,Ord)

-- | A single neo4j request. Either a request to create a Vertex or
--   a request to create an edge.
data Neo4jBatchRequest = VertexRequest Properties TemporaryId
                       | EdgeRequest Properties Label AnyId AnyId Integer deriving (Show)

-- | A neo4j batch response. It is a vector of multiple single responses.
data Neo4jBatchResponses = Neo4jBatchResponses (Vector Neo4jBatchResponse) deriving (Show)

-- | A single neo4j batch response. It consists of an id, from, location and a body.
--   Sometimes some of those fields are missing.
data Neo4jBatchResponse = Neo4jBatchResponse (Maybe Integer) Text (Maybe Text) (Maybe JSON.Value) deriving (Show)

