# CS 651 Final Project

Student Name: Felix Yifei Zong

Student Number: 20873157

## Introduction


## Correctness Validation: GraphX `triangleCount`
Example usage:
```sh
$ spark-submit --master 'local[8]' \
  --class project.BuiltInCountTriangle \
  target/assignments-1.0.jar \
  --input data/p2p-Gnutella08.txt \
  --output output/GraphXTriangleCount
```

time: `12.80s user 0.82s system 327% cpu 4.162 total`

## Triangle Type Partition Algorithm
Example usage:
```sh
$ spark-submit --master 'local[8]' \
  --class project.TriangleTypePartition \
  target/assignments-1.0.jar \
  --input data/p2p-Gnutella08.txt \
  --output output/TriangleTypePartition \
  --rho 10
```

time: `11.59s user 0.69s system 324% cpu 3.779 total`

## One Three Partition Algorithm
Example usage:
```sh
$ spark-submit --master 'local[8]' \
  --class project.OneThreePartition \
  target/assignments-1.0.jar \
  --input data/p2p-Gnutella08.txt \
  --output output/OneThreePartition \
  --rho 10
```

time: `11.35s user 0.66s system 323% cpu 3.717 total`


## Enhanced Triangle Type Partition Algorithm
Example usage:
```sh
$ spark-submit --master 'local[8]' \
  --class project.EnhancedTriangleTypePartition \
  target/assignments-1.0.jar \
  --input data/p2p-Gnutella08.txt \
  --output output/EnhancedTriangleTypePartition \
  --rho 10
```

time: `11.41s user 0.67s system 320% cpu 3.765 total`

