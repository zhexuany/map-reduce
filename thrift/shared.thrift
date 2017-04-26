namespace java edu.umn.cs.mapreduce

struct FileSplit {
    1: required string filename;
    2: required i64 offset;
    3: required i64 length;
}