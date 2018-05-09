package globesort;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.*;
import net.sourceforge.argparse4j.inf.*;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.lang.RuntimeException;
import java.lang.Exception;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GlobeSortClient {

    private final ManagedChannel serverChannel;
    private final GlobeSortGrpc.GlobeSortBlockingStub serverStub;

	private static int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    private String serverStr;

    public GlobeSortClient(String ip, int port) {
        this.serverChannel = ManagedChannelBuilder.forAddress(ip, port)
				.maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .usePlaintext(true).build();
        this.serverStub = GlobeSortGrpc.newBlockingStub(serverChannel);

        this.serverStr = ip + ":" + port;
    }

    public void run(Integer[] values) throws Exception {
        System.out.println("Pinging " + serverStr + "...");
        long t0 = System.nanoTime();
        serverStub.ping(Empty.newBuilder().build());
        long t1 = System.nanoTime();
        double t = (double)(t1-t0)/1000000000.0;
        t = t/2; //one-way

        System.out.("Ping successful.");
        System.out.println("Latency is " + t + ".\n");

        System.out.println("Requesting server to sort array");
        IntArray request = IntArray.newBuilder().addAllValues(Arrays.asList(values)).build();

        //TODO: measure total invocation time
        t0 = System.nanoTime();
        IntArray response = serverStub.sortIntegers(request);
        t1 = System.nanoTime();

        System.out.println("Sorted array.\n");
        t = (double)(t1-t0)/1000000000.0;
        System.out.println("Total invocation time of sortIntegers: " + t + " seconds.\n");

        double minusTime = response.getTime();
        System.out.println("Just sorting took this long: (response.getTime) " + minusTime + " seconds.\n");
        
        double appTP = values.length/t;
        System.out.println("Application Throughput, NumSorted/Second, is: " + appTP + ".\n" );

        double netTP = t - minusTime;
        netTP  = 4*values.length/netTP; //bytes per second
        netTP /= 2; //one way throuhgout
        System.out.println("One-Way network throughput is " + netTP + ".\n");
    }

    public void shutdown() throws InterruptedException {
        serverChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    private static Integer[] genValues(int numValues) {
        ArrayList<Integer> vals = new ArrayList<Integer>();
        Random randGen = new Random();
        for(int i : randGen.ints(numValues).toArray()){
            vals.add(i);
        }
        return vals.toArray(new Integer[vals.size()]);
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("GlobeSortClient").build()
                .description("GlobeSort client");
        parser.addArgument("server_ip").type(String.class)
                .help("Server IP address");
        parser.addArgument("server_port").type(Integer.class)
                .help("Server port");
        parser.addArgument("num_values").type(Integer.class)
                .help("Number of values to sort");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace cmd_args = parseArgs(args);
        if (cmd_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }

        Integer[] values = genValues(cmd_args.getInt("num_values"));

        GlobeSortClient client = new GlobeSortClient(cmd_args.getString("server_ip"), cmd_args.getInt("server_port"));
        try {
            client.run(values);
        } finally {
            client.shutdown();
        }
    }
}
