This project includes all notes and work
relating to the Udemy course taught by
Frank Kane's Spark with Scala course.


Each Object in the Scala folder is a 
part of some activities. They are
broken down by their relevance
and complicatedness (not a word)


##Key Points From Each Object:
###AverageFriendsByName.scala
1. parseline function ie working with a predicate function to map method that parses csv data
2. .map, .mapValues, .reduceByKey. Note with .mapValues only works with pair RDDS
3. pair RDDS, are just tuples
###MinTemperatures.scala
1. Filtering with .filter(), used mainly to eliminate rows of an RDD, an xform
2. SortBy method
3. Printing Tuples and accessing their elements ._1
4. String interpolation. Nuff said
###WordCount.scala
1. Create helper method to pass to filter method
2. Using args, sbt shell to run scripts from the terminal. **todo: pass files as args instead of strings
3. flatMap() with a .split()+regex to extract words from a txt file, and create either a much smaller RDD or larger RDD. note this is completely different than using .split()+regex with map, thats to get data from a single line of input
###PurchaseByCustomer.scala
1. .toInt & .toFloat on tuple Strings after parsing from csv file
2. .take() method for testing data (do not use collect, waste of space/time)
3. calling foreach on a set of data brought to driver program
###PopularMoviesNicer.scala
1. Broadcast variables. Learned we can send a function to cluster which can have a map of the data we will eventually want to use. Later in program we "Fold in" the movie names from the Broadcast variable into the RDD we've manipulated about.
2. Chaining parameter to the map method. Notice we can Split, pull out a specific value, and map it to a number in 1 string
3. Creating a scala Map. Notice the syntax, variable type declared 
4. this syntax for a map movieNames += (fields(0).toInt -> fields(1))
###PopularSuperHeroes.scala
1. First semi challenge I gave myself by implementing most without help
2. Learned about scala Option[] syntax, but didn't implement saw no need
3. Utilized Broadcast variable with map data structure
4. Realized .take(1) still returns an array so we needed to access data with take(1)(0)._1
5. Realized .first is much easier to deal with after a sortBy for getting min or max data, implements take for us
###Degrees of Separation (1st Serious Spark Program)
1. Learned about the mutable array syntax for Scala, and converting to immutable with .toArray syntax
2. Graphx lib: he says we can do this section of the class with Graphx library a lot more efficiently, way less code, but he wants us to start with bare bones, "first principles" so to speak
3. Learned Several Scala Syntaxes. 1. Creating own types. Say this with passing user defined types to functions to reduce verboseness.
4. Learned about Scala Option /Some/ None pattern. Used with Accumulator
5. Learned we can create an accumulator as a global variable to track if an event occurred on the cluster, such as locating a Hero. For an accumulator to be incremented, an action such as .count() must occur in the driver
6. Learned of the ++= syntax for concatenating 2 arrays
7. Very complicated for first grasp, but will get used to it
###Movie Similarities
1. First look at chaining map function to parseLine, and convert to a tuple of tuples
2. Used a join operator to get all combinations of an RDD with itself
3. Used filter after join to get rid of duplicates
4. Used groupByKey, new term to go from (K,V) -> (K, Iterable<V>)
5. This was item based collaborative filtering
6. Worked with Spark-submit and sbt
7. Taught myself cosine similarity, forgot how smart I am. Its simple, cosine is adjacent/hyp. We measure a bunch of points from each other...
###Movie Similarities 1M
1. Here all we did was learn about partitioning per # executors before doing a massive xform like groubByKey or reduceByKey
2. Learned about AWS EMR cluster, how the Yarn Hadoop is separate from MR, etc.
###SparkSQL
1. Great section introducing us to DataFrames
2. Using SparkSession instead of Context, but using Context to load initial csv file
3. Very great use for case classes in Scala, aka creating an RDD of Person objects, and converting that to a DataSet, and converting that to a DataFrame
4. DataSet created with .toDS, and we used printSchema to show what it looked like
5. We used .sql("some query") to query our DataSet and save a DataFrame
6. Learned that the method .createOrReplaceTempView creates a View with a name separate from the actual Dataset. The View is a Dataframe
    ie when we select from the View, that val wil lcontain a Dataframe
7. Learned we can convert from a Dataset[] back to an RDD by calling .rrd SIMPLE!
###Dataframes
1. Learning we can call methods directly on our Dataset such as select to get Dataframes from DataSet!!! Soo cool
2. Observe the importance of Caching for debugging / testing since we use .show 5 times (5 actions)OHHH
3. Understood that .getOrCreate must be present with SparkSession, creates new, or pulls existing one
4. All sql methods select, groupBy, show become methods
###PopularMovies(DataSets Version)
1. E.g. of How we can frame problem with Datasets for a faster, more efficient, elegant solution
2. Okay Okay very interesting. He instantiated an object in the map method. WOW That is so cool. Don't think PYthon can do that...
3. Nope, he definitely defined a movie case class.6
###Create A Spark Script with Datasets from Spark script of RDDs Part 1
1. Learned that when working with Datasets, your mapper becomes an object creator (only if unstructured), created above def's as a "final case class"
2. Found that I could not remember the syntax for inserting objects into a scala map.
3 This was with .sql syntax. We want to use 
###Create A Spark Script with Datasets from Spark script of RDDs Part 2
1. See DataFrames Object script for examples
2. Going to create my own DataSet and practice manips with log files from buganizer
3. So far I have figured out how to convert from a NDJSON (Newline Delimited JSON) to an RDD of type ROW, and converted that using a mapper to an RDD of Objects to convert to a DataSet!
4. With a ton of arbitrary data, need to map to useful data before creating objects for Dataset manipulations
5. Going to broadcast business id->name map once I can strip the json out of the file properly
6. Learned I can simply select rows from json as if it were a already structured. I guess that's the point
7. But you can only have a Dataset if you give it objects (not json)
8. It is done, however there is a partiioning problem because our dataset is too large for single computer. Testing with smaller dataset.
9. Use moviesimilarities1m and see how partitioning is done. Need a cluster
10. Work in progress, needs to be fixed.
11. I might have wasted all this time on Review Similarities when I could have simply used MLLib? Oh god
12. Ok so I solved something that had been bugging me. I learned that we can call the getOrElse method on an Option to access its result. Why so convoluted hard ti find??? Nobody may ever know
13. Learned when using var is "ok"
14. Learned about converting StringId's Int via hashcode function, very important and slick!
15. Learned that instance of casting a row object to a type with asInstanceOf(), we should instead do .as[String] and then convert to rdd, makes it much more readable
###MLLIb Introduction
MLLib is good for:
1. Classification, regression, clustering, collaborative filtering
2. Featurization
3. Pipelines
4. Persistence, saving and load algorithms, models, and Pipelines
5. Utilities like taking statistics, data handling
6. The results are very sensitive to the parameters you give it. Sometimes can be completely random
7. Putting your faith in a black box. Kinda dodgy"
8. General lesson where complicated algorithms Aren't always better
9. Notice we got far better results from the script we wrote ourselves
10. Its all about working with n and d (features). n is the number of training data points. d is the dimension of each input sample. Because d and n are reaching billions, we need to use optimization methods on them.
11. You don't want to solve the optimization problem. People just want a quick and dirty solution. Otherwise, the data will get close to solution but never exactly find it.
###Linear Regression Lecture
1. Fitting points to a line, meaning they regress towards the line. Use line to predict unobserved values
2. Usually using least squares (minimizes the squared-error between each point & the line)
3. Squared because we want to make negative values also count as poisitive values
4. Easy use, we just create a SGD (Stochastic Gradient Descent) object, and set some fields
5. SGD Does not work if your data does not fit a normal distribution, ie.. its centered around 0, so you must scale your data down
6. We will try and find a correlation between page load speed, and how much a usesr spends on the website!
7. Downside of SGD it is very common to scale down data and scale it back up to display correct values, because most data doesn't fit normal dis model...
###Linear Regression Dataset API
1. Dataframe is a dataset of row objects. Dataframe is the legacy, will hear dataset more often
2. We couldn't parse labelled points like we did in LR_RDD lecture because of a build issue so we used Vectors library line 36
3. He said importing spark.implicits._ is to "infer the schema"
4. Learned about "train and test" methods for machine learning. We can split our data in half, then use 1st to make our model, and train it on the data from 2nd that which it didn't know about
5. Ways you can skip going from RDD -> DF: 1. Importing data from a real database 2. JSON file 3. Structured streaming

###Spark Streaming (PopularHashtagsDStreams.scala)
1. First thing I noticed is println prints once, and print on stream  'objects' prints at whichever interval you specificy. This is really weird behavior  I am not used to at all
2. It is essentially the Same API, only difference is the beginning and starting parts of the program. This is because you need to set up and tear down differently. Hm, simple, makes sense. 
3.  1. Configure twitter credentials
    2. Set up a streamingContext (similar to sparkSession for DF/DS)
    3. Set up a tweet object
    4. Set up statuses (ReceiverInputDStream)
4. Don't worry about the why, accept the fact that its a bunch of RDD's with a name attached to it, don't make it more complicate than it has to be.
5. The slide duration of reducedByWindow must match parent DStream
6. Learned about the filterNot method for removing list elements by passing filterNot th e.contains() method. That super sick
7. Edit, we can put '!' before _.contains() in reg filter function and it would do same thing!
8. When doing your imports, it is important to note the IDE will recommend scala & java libraries. Meaning some methods may not show, care for this.
9. Made a really cool utility function that essentially manipulates any String element from an RDD (for sentences)
10. Learned another key abstraction, the transform method on DStreams. There is an important differentiation to make between transform and map
   ie. transform returns a new DStream by applying RDD -> RDD functions (map or flatmap) to every RDD of the source DStream currently observed. Map simply works with each individual element of the source DStream
11. ssc.start: this is when the program execution starts, and I assume it loops our code in the script above it's invocation
    ssc.awaitTermination is like ssc.stop except that it can stop from a stop() within the code as well as a ctrl+c. makes perfect sense now.
    ssc.awaitTermination is like ssc.stop except that it can stop from a stop() within the code as well as a ctrl+c. makes perfect sense now.
