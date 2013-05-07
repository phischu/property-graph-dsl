module Database.PropertyGraph.Neo4jBatch where

import Database.PropertyGraph (PropertyGraph,PG,PropertyGraphF(NewVertex,NewEdge))

import qualified Data.Aeson as JSON (Value,eitherDecode,encode,Value(Array),toJSON)

import Control.Monad.Writer.Lazy (execWriterT,WriterT,tell)
import Control.Monad.State.Lazy (evalState,State,modify,get,lift)
import Control.Monad.Free (Free(Pure,Free))
import qualified Data.Map as Map (fromList)

import Data.Text (Text)
import Control.Error (EitherT,runEitherT,scriptIO,hoistEither,fmapLT,noteT,hoistMaybe,left)
import Network.URI (URI,parseAbsoluteURI,parseRelativeReference,relativeTo)
import Network.HTTP (Request(Request),simpleHTTP,getResponseCode,getResponseBody)
import Network.HTTP.Base (ResponseCode,RequestMethod(POST))
import Network.HTTP.Headers (mkHeader,HeaderName(HdrContentLength,HdrContentType,HdrAccept))
import Network.TCP (HStream)
import Network.Stream (ConnError)
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BC (length)
import qualified Data.Vector as Vector (fromList)

type ServerAddress = String

convertPropertyGraphToNeo4jBatch :: PropertyGraph a -> JSON.Value
convertPropertyGraphToNeo4jBatch = JSON.Array . Vector.fromList . flip evalState 0 . execWriterT . interpretPropertyGraph

interpretPropertyGraph :: PropertyGraph a -> WriterT [JSON.Value] (State Integer) ()
interpretPropertyGraph (Pure _) = do
    return ()
interpretPropertyGraph (Free (NewVertex properties c)) = do
    vertexid <- lift (modify (+1) >> get)
    tell [JSON.toJSON (Map.fromList [
        ("method",JSON.toJSON "POST"),
        ("to",JSON.toJSON "/node"),
        ("id",JSON.toJSON vertexid),
        ("body",JSON.toJSON properties)])]
    interpretPropertyGraph (c vertexid)
interpretPropertyGraph (Free (NewEdge properties label from to c)) = do
    edgeid <- lift (modify (+1) >> get)
    tell [JSON.toJSON (Map.fromList [
        ("method",JSON.toJSON "POST"),
        ("to",JSON.toJSON ("{"++show from++"}/relationships")),
        ("id",JSON.toJSON edgeid),
        ("body",JSON.toJSON (Map.fromList [
            ("to",JSON.toJSON ("{"++show to++"}")),
            ("type",JSON.toJSON label),
            ("data",JSON.toJSON properties)]))])]
    interpretPropertyGraph (c edgeid)


add :: ServerAddress -> PG a -> IO (Either Neo4jBatchError JSON.Value)
add serveraddress propertygraph = runEitherT (do

    serveruri <- noteT (URIError "Failed to parse Server Address") (hoistMaybe (parseAbsoluteURI serveraddress))
    apiuri <-  noteT (URIError "Failed to parse API Address") (hoistMaybe (parseRelativeReference "db/data/batch"))

    let request = Request
            (apiuri `relativeTo` serveruri)
            POST
            [mkHeader HdrAccept "application/json",
             mkHeader HdrContentType "application/json",
             mkHeader HdrContentLength (show (BC.length body))]
            body
        body = JSON.encode (convertPropertyGraphToNeo4jBatch propertygraph)

    (code,body) <- webInteract request   `onFailure` InteractionError

    case code of
        (2,_,_) -> hoistEither (JSON.eitherDecode body) `onFailure` ResponseParseError
        _      -> left (ResponseCodeError code body))

data Neo4jBatchError = URIError String |
                       InteractionError InteractionError |
                       ResponseCodeError (Int,Int,Int) ByteString |
                       ResponseParseError String

data InteractionError = UnexpectedError String |
                        ConnectionError ConnError |
                        RetrievalError String deriving Show

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


