
package graaldemo.demo;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

public class Main {

    public static void main(String[] args) throws Exception {
        try (var context = Context.create()) {
            String src = """
                      function add(x, y) {
                        return x + y;
                      }
                    """;
            context.eval("js", src);
            Value function = context.getBindings("js").getMember("add");
            System.out.println(function.execute(1, 2));
        }
    }
}
