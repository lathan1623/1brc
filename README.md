# 1brc
My implementation for the 1 billion row challenge as a learning exercise. See https://github.com/gunnarmorling/1brc

###### Current Benchmarks:
* Original ~ 14.5s for 100m rows
* Mine ~ 10.5s for 100m rows

###### TODO:
* I think I can improve performance on the hot loop a lot by not creating string objects each iteration
* Create a custom hashmap implementation, kind of tying into the first point if I can somehow use a MappedByteBuffer region as a
key I could get rid of the creating of the byte[] arrays and Strings
* Using virtual threads right now to try it out. I think because this is CPU bound work OS threads will be more performant though
* Right now I am constrained to a 2GB file size because that is the max for MappedByteBuffer. So i'll have to have multiple buffers
and combine them or something

###### Running
* If you're interested in running my code please follow the instructions for setting up the whole project on original 1brc github repo
