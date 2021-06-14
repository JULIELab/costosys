package de.julielab.costosys.medline;

import org.testng.annotations.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.*;

public class UpdaterTest {

    @Test
    public void testDeleterLoading() {
        ServiceLoader<IDocumentDeleter> loader = ServiceLoader.load(IDocumentDeleter.class);
        assertEquals(2L, loader.stream().count());
    }

}