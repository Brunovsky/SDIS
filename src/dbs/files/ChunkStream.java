package dbs.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;

public final class ChunkStream {
  private final FileInputStream stream;
  private final Path path;

  ChunkStream(Path path) throws FileNotFoundException {
    this.path = path;
    File file = path.toFile();
    this.stream = new FileInputStream(file);
  }
}
