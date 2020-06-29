//package org.stateful.function;
//
//import org.apache.flink.statefun.sdk.Context;
//import org.apache.flink.statefun.sdk.StatefulFunction;
//import org.apache.flink.statefun.sdk.io.EgressIdentifier;
//import org.stateful.function.domain.GreetRequest;
//import org.stateful.function.domain.GreetResponse;
//
///**
// * @fileName: GreetFunction.java
// * @description: 状态函数
// * @author: by echo huang
// * @date: 2020-05-06 10:44
// */
//public class GreetFunction implements StatefulFunction {
//    @Override
//    public void invoke(Context context, Object input) {
//        GreetRequest request = (GreetRequest) input;
//        GreetResponse response = GreetResponse.newBuilder()
//                .setWho(request.getWho())
//                .setGreeting("Hello " + request.getWho())
//                .build();
//
//        context.send(new EgressIdentifier<>("state", "test", GreetResponse.class), response);
//    }
//}
