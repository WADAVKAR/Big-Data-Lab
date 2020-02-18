Assignment - Lab 3
ME16B172 - Sushant Uttam Wadavkar
1. The screenshot for logs of all the 3 tasks containing the required result - line count using VM, Cloud Functions and DataFlow, are provided below along with the python file for each task attached in the zip file.

2. The screenshot for the execution graph created by Dataflow in the background for the pipeline object created in task 3 is being attached.
   
  3. The PCollection abstraction represents a potentially distributed, multi-element data set. PCollection can be thought of as “pipeline” data; Beam transforms use PCollection objects as inputs and outputs. As such, to work on data in pipeline, it must be in the form of a PCollection. It takes input in the form of PCollection. We need to create a bucket in which output would be stored. We used the command beam.combiners.Count.Globally() to count the number of elements present in the given form of pipeline.
Issues faced and resolving them:
1. Confusion of reading and writing the files into my bucket; the problem of syntax understanding
a. Careful reading of sample syntax available on stackoverflow
b. Explanation given in the later class
2. Understanding the concept of pipeline (PCollection) was difficult through self-learning
a. The explanation at the end of next week’s lab was appropriate and helped in solving the specific doubts such as reading and writing the files into the bucket; how dataflow works in general
4. ​Unbounded Data:
The dataset in which for a particular time instance there are no bounds on the available data that is being fed to the system in this case to the pipeline, is called the unbounded data. We do not get access to entire data at once in the case of unbounded data.
PCollection application for Unbounded Data:
Windowing allows grouping operations over unbounded data collections by diving the collection into a window of finite collections according to the timestamps of the individual elements. A windowing function gave the way to assign elements to individual windows, and ways to merge those windows together. Apache beam permits us to outline different sorts of windows or the use of predefined windowing functions. For unbounded data the results are emitted when the watermark passes the end of the window indicating that the system believes all input data for the window has been processed. But there are no guarantees that data events can appear in the pipeline in the same order they were generated.
