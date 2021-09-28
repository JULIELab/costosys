package de.julielab.costosys;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Test {
    @org.testng.annotations.Test
    public void test() {
        StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        List<String> walk = walker.walk(frames -> frames
                .map(f -> f.getClassName() + "#" + f.getMethodName()).limit(3)
                .collect(Collectors.toList()));
        System.out.println(walk);
    }
}
