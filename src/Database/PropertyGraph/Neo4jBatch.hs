{-# LANGUAGE StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch (
    convertPropertyGraphToNeo4jBatch,runPropertyGraphT,
    add,Neo4jBatchError(..),
    vertexRequest,edgeRequest) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraph,
    PropertyGraphF(NewVertex,NewEdge),VertexId(VertexId),
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
import qualified Data.ByteString.Char8 as BS (pack)

import Network.Http.Client
    (Hostname,Port,openConnection,closeConnection,
     buildRequest,http,Method(POST),setAccept,setContentType,
     sendRequest,inputStreamBody,
     receiveResponse)
import System.IO.Streams.ByteString (fromLazyByteString)
import System.IO.Streams.Attoparsec (parseFromStream)

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

-- | Part of a Neo4j batch request. Create a new vertex with the given properties
--   and temporary id.
vertexRequest :: Properties -> Integer -> JSON.Value
vertexRequest properties uniqueid = JSON.toJSON (Map.fromList [
    ("method",JSON.toJSON "POST"         ),
    ("to"    ,JSON.toJSON "/node"        ),
    ("id"    ,JSON.toJSON (show uniqueid)),
    ("body"  ,JSON.toJSON properties     )])

-- | Part of a Neo4j batch request. Create a new edge with the given properties,
--   label, from the given uri to the other given uri with the given temporary
--   id.
edgeRequest :: Properties -> Label -> Text -> Text -> Integer -> JSON.Value
edgeRequest properties label fromuri touri uniqueid = JSON.toJSON (Map.fromList [
    ("method",JSON.toJSON "POST"                     ),
    ("to"    ,JSON.toJSON (fromuri<>(pack "/relationships"))),
    ("id"    ,JSON.toJSON (show uniqueid)            ),
    ("body"  ,JSON.toJSON (Map.fromList [
        ("to"    ,JSON.toJSON touri     ),
        ("type"  ,JSON.toJSON label     ),
        ("data"  ,JSON.toJSON properties)]))])

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

-- | Things that can go wrong when submitting a property graph to a neo4j database.
data Neo4jBatchError = OpeningConnectionError IOException |
                       RequestBuildingError IOException |
                       EncodingError IOException |
                       RequestSendingError IOException |
                       ResponseReceivalError IOException |
                       ConnectionClosingError IOException

deriving instance Show Neo4jBatchError

onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT


