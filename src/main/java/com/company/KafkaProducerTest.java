/*
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.company;

import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerTest {
  public static void main(String[] args) throws Exception {

    //Define properties for how the Producer finds the cluster, serializes
    //the messages and if appropriate directs the message to a specific
    //partition.

    Options options = new Options();
    options.addOption("topic", true, "Configuring the topic");
    options.addOption("fileName", true, "Configuring the file name");
    options.addOption("brokerList", true, "Configuring the broker list");

    CommandLineParser parser = new DefaultParser();

    // parse the command line arguments
    CommandLine commandline = parser.parse(options, args);
    System.out.println("topic = " + commandline.getOptionValue("topic"));
    System.out.println("filename = " + commandline.getOptionValue("topic"));
    System.out.println("brokerList = " + commandline.getOptionValue("brokerList"));

    Properties props = new Properties();
    props.put("bootstrap.servers", commandline.getOptionValue("brokerList"));
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = new KafkaProducer(props);
    logReader(commandline.getOptionValue("fileName"), commandline.getOptionValue("topic"), producer);

    producer.close();

  }

  /**
   * new function
   * reads the apache access log file, line by line
   * send each line as a message (before to the activeMQ broker)
   * close the file and exit the producer thread
   */

  public static void logReader(String inputFile, String topicName, Producer<String, String> producer) throws IOException {

    try
    {

      File file = new File(inputFile);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line = bufferedReader.readLine();

      String delims = " - - ";
      while ( line != null) {
        String tokens = bufferedReader.readLine();
        String[] str = tokens.split(delims);

        System.out.println(str[0]);
        line = bufferedReader.readLine();

        producer.send(new ProducerRecord<String, String>(topicName, str[0],str[0]));


      }

      fileReader.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
