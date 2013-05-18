{-# LANGUAGE StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch (
    convertPropertyGraphToNeo4jBatch,runPropertyGraphT,
    add,Neo4jBatchError) where

import Database.PropertyGraph.Internal (PropertyGraphT,PropertyGraph,PropertyGraphF(NewVertex,NewEdge),VertexId(VertexId),EdgeId(EdgeId))

import qualified Data.Aeson as JSON (Value(Array),eitherDecode,encode,toJSON,json)

import Control.Monad.Writer.Lazy (WriterT,runWriterT,tell)
import Control.Monad.State.Lazy (StateT,evalStateT,modify,get,lift)
import Control.Monad.Identity (Identity,runIdentity)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Error (EitherT,runEitherT,tryIO,hoistEither,fmapLT,noteT,hoistMaybe,left)
import Control.Exception (IOException)

import qualified Data.Map as Map (fromList)
import qualified Data.Vector as Vector (fromList)
import Data.Text (Text,unpack)
import Data.ByteString.Char8 (pack)
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as B (length)

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
--   as the second part of the result.
runPropertyGraphT :: (Monad m,Functor m) => PropertyGraphT m a -> m (a,JSON.Value)
runPropertyGraphT =
    fmap (fmap (JSON.Array . Vector.fromList)) .
    flip evalStateT 0 .
    runWriterT .
    interpretPropertyGraph

interpretPropertyGraph :: (Monad m) => PropertyGraphT m a -> WriterT [JSON.Value] (StateT Integer m) a
interpretPropertyGraph propertygraph = do
    next <- lift (lift (runFreeT propertygraph))

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            vertexid <- lift (modify (+1) >> get) >>= return . VertexId
            tell [JSON.toJSON (Map.fromList [
                ("method",JSON.toJSON "POST"    ),
                ("to"    ,JSON.toJSON "/node"   ),
                ("id"    ,JSON.toJSON vertexid  ),
                ("body"  ,JSON.toJSON properties)])]
            interpretPropertyGraph (continue vertexid)

        Free (NewEdge properties label (VertexId from) (VertexId to) continue) -> do
            edgeid <- lift (modify (+1) >> get) >>= return . EdgeId
            tell [JSON.toJSON (Map.fromList [
                ("method",JSON.toJSON "POST"                             ),
                ("to"    ,JSON.toJSON ("{"++show from++"}/relationships")),
                ("id"    ,JSON.toJSON edgeid                             ),
                ("body"  ,JSON.toJSON (Map.fromList [
                    ("to"    ,JSON.toJSON ("{"++show to++"}")),
                    ("type"  ,JSON.toJSON label              ),
                    ("data"  ,JSON.toJSON properties         )]))])]
            interpretPropertyGraph (continue edgeid)

-- | Add a property graph to a neo4j database.
add :: Hostname -> Port -> JSON.Value -> IO (Either Neo4jBatchError JSON.Value)
add hostname port neo4jbatch = runEitherT (do

            connection <- tryIO (openConnection hostname port) `onFailure` OpeningConnectionError

            request <- tryIO (buildRequest (do
                http POST (pack "/db/data/batch")
                setAccept (pack "application/json")
                setContentType (pack "application/json")
                )) `onFailure` RequestBuildingError

            bodyInputStream <- tryIO (fromLazyByteString (JSON.encode (
                neo4jbatch))) `onFailure` EncodingError

            tryIO (sendRequest connection request (inputStreamBody bodyInputStream)) `onFailure` RequestSendingError

            jsonvalue <- tryIO (receiveResponse connection (\response responsebody -> (do
                parseFromStream JSON.json responsebody))) `onFailure` ResponseReceivalError

            tryIO (closeConnection connection) `onFailure` ConnectionClosingError

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


