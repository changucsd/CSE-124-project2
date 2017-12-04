package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.Builder;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    private void ensure(boolean b) {
	if (b == false) {
		throw new RuntimeException("Assertion failed!");
	}
    }

    private static Block stringToBlock(String s) {
	Builder builder = Block.newBuilder();

	try {
		builder.setData(ByteString.copyFrom(s, "UTF-8"));
	} catch (UnsupportedEncodingException e) {
		throw new RuntimeException(e);
	}

	builder.setHash(HashUtils.sha256(s));

	return builder.build(); // turns the Builder into a Block
    }
///////////////////////////////////////////////////////////////////////////////////////////////////////
    private void go() {

     //test_Block();
     //test_md_centralized_filenotfound();
     test_md_centralized_missingblocks();


    }

    private void test_Block() {
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
		Block b1 = stringToBlock("block_01");
		Block b2 = stringToBlock("block_02");

		ensure(blockStub.hasBlock(b1).getAnswer() == false);
		ensure(blockStub.hasBlock(b2).getAnswer() == false);
		
		blockStub.storeBlock(b1);
		ensure(blockStub.hasBlock(b1).getAnswer() == true);

		blockStub.storeBlock(b2);
		ensure(blockStub.hasBlock(b2).getAnswer() == true);

		Block b1prime = blockStub.getBlock(b1);
		ensure(b1prime.getHash().equals(b1.getHash()));
		ensure(b1.getData().equals(b1.getData()));

		logger.info("We passed all the tests... yay!");

    }


    private void test_md_centralized_filenotfound() {
		
		metadataStub.ping(Empty.newBuilder().build());
		logger.info("Running test test_md_centralized_filenotfound");
		
		// test for a non-existant file
		FileInfo nonExistantFile = FileInfo.newBuilder().setFilename("notfound.txt").build();
		FileInfo nonExistantFileResult = metadataStub.readFile(nonExistantFile);
		ensure(nonExistantFileResult.getFilename().equals("notfound.txt"));
		ensure(nonExistantFileResult.getVersion() == 0);
		
        logger.info("test_md_centralized_filenotfound test passed... yay!");
    }

 
   
    private void test_md_centralized_missingblocks() {
		
		metadataStub.ping(Empty.newBuilder().build());
		logger.info("Running test test_md_centralized_missingblocks");
		
		// file cat.txt
		
		// test for a file with a good version, but missing blocks
		Block cat_b0 = stringToBlock("cat_block0");
		Block cat_b1 = stringToBlock("cat_block1");
		Block cat_b2 = stringToBlock("cat_block2");
		
		ArrayList<String> cathashlist = new ArrayList<String>();
		cathashlist.add(cat_b0.getHash());
		cathashlist.add(cat_b1.getHash());
		cathashlist.add(cat_b2.getHash());
		
		surfstore.SurfStoreBasic.FileInfo.Builder catBuilder = FileInfo.newBuilder();
		catBuilder.setFilename("cat.txt");
		catBuilder.setVersion(1);
		catBuilder.addAllBlocklist(cathashlist);
		FileInfo catreq = catBuilder.build();

		/* test on readFile when file is not on record*/
                FileInfo readResult = metadataStub.readFile(catreq);
                ensure(readResult.getFilename().equals("cat.txt"));
                ensure(readResult.getVersion() == 0);
                
                /* test on modifyFile*/
		WriteResult catresult = metadataStub.modifyFile(catreq);
		ensure(catresult.getResult().equals(Result.MISSING_BLOCKS));
		ensure(catresult.getMissingBlocksCount() == 3);
		
		blockStub.storeBlock(cat_b0);
		catresult = metadataStub.modifyFile(catreq);
		ensure(catresult.getResult().equals(Result.MISSING_BLOCKS));
		ensure(catresult.getMissingBlocksCount() == 2);
		
		blockStub.storeBlock(cat_b1);
		catresult = metadataStub.modifyFile(catreq);
		ensure(catresult.getResult().equals(Result.MISSING_BLOCKS));
		ensure(catresult.getMissingBlocksCount() == 1);
		
		blockStub.storeBlock(cat_b2);
		catresult = metadataStub.modifyFile(catreq);
		ensure(catresult.getResult().equals(Result.OK));
		
                /* test on readFile when file is on record*/
                readResult = metadataStub.readFile(catreq);
                ensure(readResult.getFilename().equals("cat.txt"));
                ensure(readResult.getVersion() == 1);

                /* test on deleteFile*/
	        surfstore.SurfStoreBasic.FileInfo.Builder myBuilder = FileInfo.newBuilder();
     
                FileInfo myreq1 = myBuilder.setFilename("cat.txt").setVersion(1).build();
                WriteResult deleteResult = metadataStub.deleteFile(myreq1);
                ensure(deleteResult.getResult().equals(Result.OLD_VERSION));  // v should be 2
                ensure(deleteResult.getCurrentVersion() == 1);
                
                FileInfo myreq2 = myBuilder.setFilename("cat.txt").setVersion(2).build();
		deleteResult = metadataStub.deleteFile(myreq2);
                ensure(deleteResult.getResult().equals(Result.OK));  
                ensure(deleteResult.getCurrentVersion() == 2);


                logger.info("test_md_centralized_missingblocks test passed... yay!");

    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        
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

        Client client = new Client(config);
        
        try {
        	client.go();
        } finally {
            client.shutdown();
        }
    }

}
