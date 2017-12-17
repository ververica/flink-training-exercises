package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;

public interface TrainingExerciseBase<IN, OUT> {
	DataStream<IN> testStream();
	DataStream<IN> realStream(String[] args);

	ArrayList<OUT> expectedResult();
	DataStream<OUT> perform(DataStream<IN> input);

	void run(DataStream<IN> input) throws Exception;
	void test(DataStream<IN> input) throws Exception;
}
