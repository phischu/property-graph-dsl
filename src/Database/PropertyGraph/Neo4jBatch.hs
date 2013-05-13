{-# LANGUAGE StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch (
    convertPropertyGraphToNeo4jBatch,
    add,Neo4jBatchError) where

import Database.PropertyGraph.Internal (PropertyGraph,PG,PropertyGraphF(NewVertex,NewEdge),VertexId(VertexId),EdgeId(EdgeId))

import qualified Data.Aeson as JSON (Value(Array),eitherDecode,encode,toJSON)

import Control.Monad.Writer.Lazy (WriterT,execWriterT,tell)
import Control.Monad.State.Lazy (State,evalState,modify,get,lift)
import Control.Monad.Free (Free(Pure,Free))
import Control.Error (EitherT,runEitherT,tryIO,hoistEither,fmapLT,noteT,hoistMaybe,left)
import Control.Exception (IOException)

import qualified Data.Map as Map (fromList)
import qualified Data.Vector as Vector (fromList)
import Data.Text (Text,unpack)
import Data.ByteString.Char8 (pack)
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as B (length)

import Network.Http.Client
    (Hostname,Port,withConnection,openConnection,
     buildRequest,http,Method(POST),setAccept,setContentType,
     sendRequest,inputStreamBody,
     receiveResponse)
import System.IO.Streams (connect)
import System.IO.Streams.Handle (stdout)
import System.IO.Streams.ByteString (fromLazyByteString)

-- | Convert a given property graph to the equivalent body of a neo4j batch request.
convertPropertyGraphToNeo4jBatch :: PropertyGraph a -> JSON.Value
convertPropertyGraphToNeo4jBatch =
    JSON.Array .
    Vector.fromList .
    flip evalState 0 .
    execWriterT .
    interpretPropertyGraph

interpretPropertyGraph :: PropertyGraph a -> WriterT [JSON.Value] (State Integer) ()
interpretPropertyGraph (Pure _) = do
    return ()

interpretPropertyGraph (Free (NewVertex properties c)) = do

    vertexid <- lift (modify (+1) >> get) >>= return . VertexId

    tell [JSON.toJSON (Map.fromList [
        ("method",JSON.toJSON "POST"    ),
        ("to"    ,JSON.toJSON "/node"   ),
        ("id"    ,JSON.toJSON vertexid  ),
        ("body"  ,JSON.toJSON properties)])]

    interpretPropertyGraph (c vertexid)

interpretPropertyGraph (Free (NewEdge properties label (VertexId from) (VertexId to) c)) = do

    edgeid <- lift (modify (+1) >> get) >>= return . EdgeId

    tell [JSON.toJSON (Map.fromList [
        ("method",JSON.toJSON "POST"                             ),
        ("to"    ,JSON.toJSON ("{"++show from++"}/relationships")),
        ("id"    ,JSON.toJSON edgeid                             ),
        ("body"  ,JSON.toJSON (Map.fromList [
            ("to"    ,JSON.toJSON ("{"++show to++"}")),
            ("type"  ,JSON.toJSON label              ),
            ("data"  ,JSON.toJSON properties         )]))])]

    interpretPropertyGraph (c edgeid)

-- | Add a property graph to a neo4j database.
add :: Hostname -> Port -> PropertyGraph a -> IO (Either Neo4jBatchError ())
add hostname port propertygraph = withConnection (openConnection hostname port) (\connection -> (do
        runEitherT (do

            request <- tryIO (buildRequest (do
                http POST (pack "/db/data/batch")
                setAccept (pack "application/json")
                setContentType (pack "application/json")
                )) `onFailure` RequestBuildingError

            bodyInputStream <- tryIO (fromLazyByteString (JSON.encode (
                convertPropertyGraphToNeo4jBatch propertygraph))) `onFailure` EncodingError

            tryIO (sendRequest connection request (inputStreamBody bodyInputStream)) `onFailure` RequestSendingError

            tryIO (receiveResponse connection (\response responsebody -> (do
                connect responsebody stdout))) `onFailure` ResponseReceivalError
            )))

-- | Things that can go wrong when submitting a property graph to a neo4j database.
data Neo4jBatchError = RequestBuildingError IOException |
                       EncodingError IOException |
                       RequestSendingError IOException |
                       ResponseReceivalError IOException |
                       ResponseCodeError (Int,Int,Int) ByteString |
                       ResponseParseError String

deriving instance Show Neo4jBatchError
{-
-- | Things that can go wrong when doing an http interaction.
data InteractionError = UnexpectedError IOException |
                        ConnectionError ConnError |
                        RetrievalError IOException

deriving instance Show InteractionError

type Body s = s

webInteract :: (HStream s) => Request s -> EitherT InteractionError IO (ResponseCode,Body s)
webInteract request = do
    result   <- tryIO (simpleHTTP request)               `onFailure` UnexpectedError
    response <- hoistEither result                       `onFailure` ConnectionError
    code     <- tryIO (getResponseCode (Right response)) `onFailure` RetrievalError
    body     <- tryIO (getResponseBody (Right response)) `onFailure` RetrievalError
    return (code,body)
-}
onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT


