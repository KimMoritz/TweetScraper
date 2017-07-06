package storm.camel;

import org.apache.camel.builder.RouteBuilder;

public final class StreamingRoute extends RouteBuilder {

    @Override
    public final void configure() throws Exception {
        from("activemq:HashtagFromScraperQueue")
                .to("websocket://storm?sendToAll=true");
    }
}
