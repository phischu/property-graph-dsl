{-# LANGUAGE StandaloneDeriving #-}
module Database.PropertyGraph.Neo4jBatch (
    convertPropertyGraphToNeo4jBatch,
    add,ServerAddress,Neo4jBatchError,InteractionError) where

import Database.PropertyGraph.Internal (PropertyGraph,PG,PropertyGraphF(NewVertex,NewEdge),VertexId(VertexId),EdgeId(EdgeId))

import qualified Data.Aeson as JSON (Value(Array),eitherDecode,encode,toJSON)

import Control.Monad.Writer.Lazy (WriterT,execWriterT,tell)
import Control.Monad.State.Lazy (State,evalState,modify,get,lift)
import Control.Monad.Free (Free(Pure,Free))
import Control.Error (EitherT,runEitherT,scriptIO,hoistEither,fmapLT,noteT,hoistMaybe,left)

import qualified Data.Map as Map (fromList)
import qualified Data.ByteString.Lazy.Char8 as BC (length)
import qualified Data.Vector as Vector (fromList)
import Data.Text (Text,unpack)
import Data.ByteString.Lazy (ByteString)

import Network.HTTP (Request(Request),simpleHTTP,getResponseCode,getResponseBody)
import Network.HTTP.Base (ResponseCode,RequestMethod(POST))
import Network.HTTP.Headers (mkHeader,HeaderName(HdrContentLength,HdrContentType,HdrAccept))
import Network.URI (URI,parseAbsoluteURI,parseRelativeReference,relativeTo)
import Network.TCP (HStream)
import Network.Stream (ConnError)

-- | The address of a neo4j server for example "http://localhost:7474"
type ServerAddress = Text

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
add :: ServerAddress -> PropertyGraph a -> IO (Either Neo4jBatchError JSON.Value)
add serveraddress propertygraph = runEitherT (do

    serveruri <- noteT (URIError "Failed to parse Server Address") (hoistMaybe (parseAbsoluteURI (unpack serveraddress)))
    apiuri    <- noteT (URIError "Failed to parse API Address")    (hoistMaybe (parseRelativeReference "db/data/batch"))

    let request = Request uri verb headers body
        uri = (apiuri `relativeTo` serveruri)
        verb = POST
        headers = [
            mkHeader HdrAccept "application/json",
            mkHeader HdrContentType "application/json",
            mkHeader HdrContentLength (show (BC.length body))]
        body = JSON.encode (convertPropertyGraphToNeo4jBatch propertygraph)

    (code,body) <- webInteract request `onFailure` InteractionError

    case code of
        (2,_,_) -> hoistEither (JSON.eitherDecode body) `onFailure` ResponseParseError
        _       -> left (ResponseCodeError code body))

-- | Things that can go wrong when submitting a property graph to a neo4j database.
data Neo4jBatchError = URIError String |
                       InteractionError InteractionError |
                       ResponseCodeError (Int,Int,Int) ByteString |
                       ResponseParseError String

deriving instance Show Neo4jBatchError

-- | Things that can go wrong when doing an http interaction.
data InteractionError = UnexpectedError String |
                        ConnectionError ConnError |
                        RetrievalError String

deriving instance Show InteractionError

type Body s = s

webInteract :: (HStream s) => Request s -> EitherT InteractionError IO (ResponseCode,Body s)
webInteract request = do
    result   <- scriptIO (simpleHTTP request)               `onFailure` UnexpectedError
    response <- hoistEither result                          `onFailure` ConnectionError
    code     <- scriptIO (getResponseCode (Right response)) `onFailure` RetrievalError
    body     <- scriptIO (getResponseBody (Right response)) `onFailure` RetrievalError
    return (code,body)

onFailure :: Monad m => EitherT a m r -> (a -> b) -> EitherT b m r
onFailure = flip fmapLT


