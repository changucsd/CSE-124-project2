package surfstore;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.Builder;


import surfstore.SurfStoreBasic.FileInfo;
//import surfstore.SurfStoreBasic.FileInfo.Builder;

import surfstore.SurfStoreBasic.WriteResult;
//import surfstore.SurfStoreBasic.WriteResult.Builder;

import com.google.protobuf.ByteString;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
    protected ConfigReader config;
    
    private static  ManagedChannel blockChannel;
    private static  BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    
    /* distributed system */
    private static ManagedChannel metadataChannel1;
    private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub1;

    private static ManagedChannel metadataChannel2;
    private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub2;
  
    private static  boolean leadIng;

    public MetadataStore(ConfigReader config, int assignedId) {
        
        if(config.getNumMetadataServers() > 1)       // we have a distributed version
        {
            int leaderNumber = config.getLeaderNum();

            int metaOne = config.getMetadataPort(1);
            int metaTwo = config.getMetadataPort(2);
            int metaThree = config.getMetadataPort(3);

            if(leaderNumber == assignedId)          // this server is the leader
            {
                this.leadIng = true;

                if (leaderNumber == 1){
              
                    this.metadataChannel1 = ManagedChannelBuilder.forAddress("127.0.0.1", metaTwo
                      .usePlaintext(true).build();
                    this.metadataStub1 = MetadataStoreGrpc.newBlockingStub(metadataChannel1);

                    this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", metaThree
                      .usePlaintext(true).build();
                    this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);
                }

                else if (leaderNumber == 2) {

                    this.metadataChannel1 = ManagedChannelBuilder.forAddress("127.0.0.1", metaOne
                      .usePlaintext(true).build();
                    this.metadataStub1 = MetadataStoreGrpc.newBlockingStub(metadataChannel1);

                    this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", metaThree
                      .usePlaintext(true).build();
                    this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);
              
                }

                else if (leaderNumber == 3) {

                    this.metadataChannel1 = ManagedChannelBuilder.forAddress("127.0.0.1", metaTwo
                      .usePlaintext(true).build();
                    this.metadataStub1 = MetadataStoreGrpc.newBlockingStub(metadataChannel1);

                    this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", metaOne
                      .usePlaintext(true).build();
                    this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);
              
                }
            }

            else                                    // this server is not leader
            {
                this.leadIng = false;

                this.metadataChannel1 = null;
                this.metadataStub1 = null;

                this.metadataChannel2 = null;
                this.metadataStub2 = null;
            }


        }

       
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
    	  this.config = config;
	}

    private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        int myid = c_args.getInt("number");
        logger.info("Metadata server number " + myid + " starting");

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config, myid);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }
   
    public static class Info{
       
        public int version;
        public List<String> hashList;      
  
        public Info(){
            this.version = 0;
            this.hashList = new ArrayList<String>();
        }

    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        
       		protected Map<String, Info> storedFile;
          protected boolean crushed;
          

      	public MetadataStoreImpl() {
		      super();
			    this.storedFile = new HashMap<String, Info>();
          this.crushed = false;
          
		    }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
          		     io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
          //asyncUnimplementedUnaryCall(METHOD_READ_FILE, responseObserver);

                logger.info("Read file with name:" + request.getFilename());

          	String fileName = request.getFilename();
    
          	FileInfo.Builder builder = FileInfo.newBuilder();
                
                if(fileName != null && storedFile.containsKey(fileName) == true )
                {
                   Info existingFile = storedFile.get(fileName);

                   int version = existingFile.version;
                   
                   builder.setFilename(fileName);
                   builder.setVersion(version);
                   builder.addAllBlocklist(new ArrayList<String>(existingFile.hashList));

                }

                else // the file does not exist, return version 0
                {
                                    
                   builder.setFilename(fileName);
                   builder.setVersion(0);
                             
                }
                
	        FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
         		       io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

           //asyncUnimplementedUnaryCall(METHOD_MODIFY_FILE, responseObserver);
	        logger.info("Modify file with name:" + request.getFilename());
               

            int version = request.getVersion();
            String fileName = request.getFilename();
            List<String> requestBlocklist = new ArrayList<String>(request.getBlocklistList());
                
                                
            WriteResult.Builder builder = WriteResult.newBuilder();

            /* check BlockStore inforamtion */
            List<String> missingHash = new ArrayList<String>();

            for(String eachHash: requestBlocklist)
            {
               
                Block.Builder sentBlock = Block.newBuilder();
                sentBlock.setHash(eachHash);
                Block myBlock = sentBlock.build();

                if(blockStub.hasBlock(myBlock).getAnswer() == false)
                {
                    missingHash.add(eachHash);
                }
            }

             /*check version first*/
            int currentVersion = 0;

            if(fileName != null && storedFile.containsKey(fileName) == true)  // the file has been created
            {  
                Info existingFile = storedFile.get(fileName);
                currentVersion = existingFile.version;

                if(version == currentVersion + 1)    // version checked
                {

                    if(missingHash.isEmpty() ==  true)  // missingblock checked
                    {
                        existingFile.hashList.clear();   //  clear the blocks inside the Info
                       
                        for(String each : requestBlocklist)
                        {
                            existingFile.hashList.add(each);
                        }

                        existingFile.version = version;
                                        
                        builder.setResultValue(0);
                        builder.setCurrentVersion(currentVersion);   // blockstore checked, verion checked
                    }
                        
                    else                                // there is missingblocks
                    {

                         builder.setResultValue(2);
                         builder.setCurrentVersion(currentVersion);
                         builder.addAllMissingBlocks(new ArrayList<String>(missingHash));

                    }            


                      
                }

                else                                                         // fail to modify because of version
                {
                                     
                    builder.setResultValue(1);                                // version check fail with file existed
                    builder.setCurrentVersion(currentVersion); 
                }

            
            }

            else                                                              //file has not been created, so verion is 0
            {
                
                if(version == currentVersion + 1)    // version checked (version must be 1)
                {

                    if(missingHash.isEmpty() ==  true)  // missingblock checked and build a new file 
                    {
                        Info newFile = new Info();
                        newFile.version = version;
                        newFile.hashList = new ArrayList<String>(requestBlocklist);
                                             
                        storedFile.put(fileName,newFile);

                        builder.setResultValue(0);                                
                        builder.setCurrentVersion(newFile.version); 
                    }
                        
                    else                                // there is missingblocks, dont create file and send 0 back
                    {

                         builder.setResultValue(2);
                         builder.setCurrentVersion(currentVersion);
                         builder.addAllMissingBlocks(new ArrayList<String>(missingHash));

                    }            


                      
                }

                else                                                         // fail to modify because of version
                {                                   
                    builder.setResultValue(1);                                // version check fail and send 0 back
                    builder.setCurrentVersion(currentVersion); 
                }


            }
   
            WriteResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted(); 


                     
        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
          		       io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

          //asyncUnimplementedUnaryCall(METHOD_DELETE_FILE, responseObserver);
          logger.info("Delete file with name:" + request.getFilename());


                int version = request.getVersion();
                String fileName = request.getFilename();
                List<String> blockList = request.getBlocklistList();

            	WriteResult.Builder builder = WriteResult.newBuilder();
                
                if(fileName != null && storedFile.containsKey(fileName) == true)  // the file has been created
                {   
                     Info existingFile = storedFile.get(fileName);
                     if(version == existingFile.version + 1)
                     {
                         existingFile.hashList.clear();
                         existingFile.hashList.add("0");
                         existingFile.version = version;
 
                         builder.setResultValue(0);                                  // successfully deleted
                         builder.setCurrentVersion(version); 
                        
                     } 
                     else
                     {
                         
                         builder.setResultValue(1);                                  // fail to delete
                         builder.setCurrentVersion(existingFile.version); 

                     } 
                }

                else                                                            // the file has never been created
                {
                     if(version == 0 + 1)
                     {
                          Info newFile = new Info();
                          newFile.version = version;
                          newFile.hashList.add("0");                                     
                          storedFile.put(fileName,newFile);
                          builder.setResultValue(0);                                
		          builder.setCurrentVersion(newFile.version); 

                     }
                     else
                     {

                          builder.setResultValue(1);                                  // fail to delete
                          builder.setCurrentVersion(0); 

                     }

                }
 
                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

        }
         
        
        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
          		    io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

           //asyncUnimplementedUnaryCall(METHOD_IS_LEADER, responseObserver);

                SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(leadIng).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
           
        }
        

        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                  io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {

            this.crushed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            

        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                  io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            
            this.crushed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
                
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                  io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            

             SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.crushed).build();
             responseObserver.onNext(response);
             responseObserver.onCompleted();
                
        }
}
