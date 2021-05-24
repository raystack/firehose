package io.odpf.firehose.sink.file;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.nio.file.Path;

@Getter
@NoArgsConstructor
@EqualsAndHashCode
public class PathBuilder {
    private Path dir;
    private String fileName;

    public PathBuilder setDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public PathBuilder setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public static PathBuilder create() {
        return new PathBuilder();
    }

    public String build(){
        return dir.resolve(fileName).toString();
    }

    public PathBuilder copy(){
        return new PathBuilder().setDir(dir).setFileName(fileName);
    }
}
