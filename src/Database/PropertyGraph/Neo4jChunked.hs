module Database.PropertyGraph.Neo4jChunked where
{-
import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraphF(NewVertex,NewEdge),
    VertexId(VertexId,unVertexId))
import Database.PropertyGraph.Neo4jBatch (add,vertexRequest,edgeRequest)



import Control.Proxy (Proxy,Producer,Consumer,liftP,request,respond,(>->),(>=>),toListD,unitU)
import Control.Proxy.Trans.State (StateP,modify,get)

import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)

import Control.Error.Util (note,hush)
import Control.Error.Safe (lastErr,readErr)

import Data.Map (Map)
import Data.List.Split (splitOn)
import qualified Data.Map as Map (lookup,union,empty,insert)
import qualified Data.HashMap.Strict as HashMap (lookup)
import qualified Data.Vector as Vector (foldM')
import Data.Text (pack,unpack)

import qualified Data.Aeson as JSON (Value(Array,Number,String,Object),json,toJSON)
import qualified Data.Attoparsec.Number as Attoparsec (Number(I))

import Network.URI (parseURI,uriPath)

-- | Interpret a property graph as the list of neo4j requests and a running unique Id.
interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> () ->
    Producer (StateP Integer (StateP (Map Integer Integer) p)) JSON.Value m r
interpretPropertyGraphT propertygraph () = do

    temporaryid <- modify (+1) >> get

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            respond (vertexRequest properties temporaryid)
            interpretPropertyGraphT (continue (VertexId temporaryid)) ()

        Free (NewEdge properties label fromid toid continue) -> do

            vertexidmap <- liftP get
            let temporaryOrExisting tempid = case Map.lookup tempid vertexidmap of
                    Nothing         -> pack ("{"++show tempid++"}")
                    Just existingid -> pack ("/node/"++show existingid)
                fromuri = temporaryOrExisting (unVertexId fromid)
                touri   = temporaryOrExisting (unVertexId toid)

            respond (edgeRequest properties label fromuri touri temporaryid)
            interpretPropertyGraphT continue ()



-- | In a neo4j batch insertion response find a match between temporary
--   ids and new ids.
matchTemporaryIds :: JSON.Value -> Either String (Map Integer Integer)
matchTemporaryIds (JSON.Array array) = note "Extracting neo4j response info failed" (Vector.foldM' insertIds Map.empty array)
matchTemporaryIds _                  = Left "Neo4j batch response is not an array"

-- | Extract the tempid existingid pair from the given json object and insert it
--   into the given map.
insertIds :: Map Integer Integer -> JSON.Value -> Maybe (Map Integer Integer)
insertIds vertexidmap object = do
    (temporaryid,existingid) <- extractIds object
    return (Map.insert temporaryid existingid vertexidmap)

-- | From a part of a neo4j batch insertion response extract the id and
--   the id part of the location.
extractIds :: JSON.Value -> Maybe (Integer,Integer)
extractIds (JSON.Object object) = do
    tempidvalue <- HashMap.lookup (pack "id") object
    tempid <- case tempidvalue of
        JSON.Number (Attoparsec.I tempid) -> Just tempid
        _                 -> Nothing
    existingidvalue <- HashMap.lookup (pack "location") object
    existingiduriststring <- case existingidvalue of
        JSON.String existingiduriststring -> Just existingiduriststring
        _                    -> Nothing
    existingiduri <- parseURI (unpack existingiduriststring)
    existingidstring <- hush (lastErr "last of empty list" (splitOn "/" (uriPath existingiduri)))
    existingid <- hush (readErr "cannot read id" existingidstring)
    return (tempid,existingid)

chunk p = (liftP . p >-> toListD >-> unitU) >=> respond
-}