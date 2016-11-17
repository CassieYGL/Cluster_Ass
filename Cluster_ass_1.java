/* @Author Yuge LIANG Student ID 713706*/

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import mpi.*;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import java.nio.CharBuffer;

import java.nio.MappedByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.charset.Charset;

import java.nio.charset.CharsetDecoder;

public class Cluster {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		try {

			MPI.Init(args);
			int count_term = 0;
			String[] ss = null;
			String readin = null;
			String term = args[3];
			int countUser = 0;
			int countTopic = 0;
			int[] eachCount = new int[1];
			Map<String, Integer> mapUsers = new HashMap<String, Integer>();
			Map<String, Integer> mapTopics = new HashMap<String, Integer>();

			int size = MPI.COMM_WORLD.Size();

			int myrank = MPI.COMM_WORLD.Rank();

			File file = new File("twitter.csv");
			long length = file.length();

			

			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			BufferedReader in = new BufferedReader(new InputStreamReader(bis, "utf-8"), 5 * 1024 * 1024);
			long time = System.currentTimeMillis();
			if(size==1){
				while((readin=in.readLine())!=null){
					
					if (readin.contains(term)) {
						eachCount[0] += 1;
					}
					ss = readin.replaceAll("[:,\",{,}]+", " ").split(" ");

					for (int i = 0; i < ss.length; i++) {

						if (ss[i].startsWith("@")) {
							if (mapUsers.containsKey(ss[i])) {
								countUser = mapUsers.get(ss[i]) + 1;
								mapUsers.put(ss[i], countUser);
							} else {
								mapUsers.put(ss[i], 1);
							}
						} else if (ss[i].startsWith("#")) {
							if (mapTopics.containsKey(ss[i])) {
								countTopic = mapTopics.get(ss[i]) + 1;
								mapTopics.put(ss[i], countTopic);
							} else {
								mapTopics.put(ss[i], 1);
							}
						}
					}	
					
				}
				
				List<Map.Entry<String, Integer>> userList = new ArrayList<Map.Entry<String, Integer>>(
						mapUsers.entrySet());
				Collections.sort(userList, new Comparator<Map.Entry<String, Integer>>() {

					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {

						return o2.getValue().compareTo(o1.getValue());
					}
				});

				List<Map.Entry<String, Integer>> topicList = new ArrayList<Map.Entry<String, Integer>>(
						mapTopics.entrySet());
				Collections.sort(topicList, new Comparator<Map.Entry<String, Integer>>() {

					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {

						return o2.getValue().compareTo(o1.getValue());
					}
				});
				int t = 0;

				for (Map.Entry<String, Integer> mapping1 : userList) {
					if (t < 10)
						System.out.println(mapping1.getKey() + ":" + mapping1.getValue());
					t++;
				}

				t = 0;
				for (Map.Entry<String, Integer> mapping2 : topicList) {
					if (t < 10)
						System.out.println(mapping2.getKey() + ":" + mapping2.getValue());
					t++;
				}

				System.out.println("Your term \" " + term + " \" appears " + eachCount[0] + " times.");

				System.out.println("total time is " + (System.currentTimeMillis() - time)  + " msec");
				
				
				
			}
			
			
			
			
			
			
			else{
			long chunk = length % size == 0 ? length / size : ((int) (Math.ceil((double) length / size)));
			
			

			long start = myrank * chunk;
			long end = (start + chunk) > length ? length : (start + chunk);

			
			in.ready();
			in.skip(chunk * myrank);

			ArrayList<String> usrname = new ArrayList<String>();
			ArrayList<Integer> usrcounter = new ArrayList<Integer>();

			ArrayList<String> topics = new ArrayList<String>();
			ArrayList<Integer> topicounter = new ArrayList<Integer>();

			int k = 0;
			String[] usrlist = null;

			int[] countOne = null;
			String[] tpclist = null;
			int[] countTwo = null;

			long p = 0;

			while (p < chunk && (readin = in.readLine()) != null) {
				if (readin.contains(term)) {
					eachCount[0] += 1;
				}
				
				ss = readin.replaceAll("[:,\",{,}]+", " ").split(" ");

				for (int i = 0; i < ss.length; i++) {

					if (ss[i].startsWith("@")) {
						if (mapUsers.containsKey(ss[i])) {
							countUser = mapUsers.get(ss[i]) + 1;
							mapUsers.put(ss[i], countUser);
						} else {
							mapUsers.put(ss[i], 1);
						}
					} else if (ss[i].startsWith("#")) {
						if (mapTopics.containsKey(ss[i])) {
							countTopic = mapTopics.get(ss[i]) + 1;
							mapTopics.put(ss[i], countTopic);
						} else {
							mapTopics.put(ss[i], 1);
						}
					}
				}
				p += readin.getBytes().length;
			}
			
			if (myrank != 0) {

				MPI.COMM_WORLD.Isend(eachCount, 0, 1, MPI.INT, 0, 1);

				Iterator it = mapUsers.keySet().iterator();

				while (it.hasNext()) {
					String key = (String) it.next();
					usrname.add(key);
					usrcounter.add(mapUsers.get(key));
					k++;
				}

				Iterator its = mapTopics.keySet().iterator();
				int t = 0;
				while (its.hasNext()) {
					String key = (String) its.next();
					topics.add(key);
					topicounter.add(mapTopics.get(key));
					k++;
				}

				usrlist = new String[usrname.size()];

				countOne = new int[usrcounter.size()];
				tpclist = new String[topics.size()];
				countTwo = new int[topicounter.size()];

				for (int u = 0; u < usrname.size(); u++) {

					usrlist[u] = usrname.get(u);
				}

				for (int u = 0; u < usrcounter.size(); u++) {
					countOne[u] = usrcounter.get(u);
				}

				for (int u = 0; u < topics.size(); u++) {
					tpclist[u] = topics.get(u);
				}

				for (int u = 0; u < topicounter.size(); u++) {
					countTwo[u] = topicounter.get(u);
				}

				MPI.COMM_WORLD.Isend(usrlist, 0, usrlist.length, MPI.OBJECT, 0, 2);

				MPI.COMM_WORLD.Isend(countOne, 0, countOne.length, MPI.INT, 0, 3);
				MPI.COMM_WORLD.Isend(tpclist, 0, tpclist.length, MPI.OBJECT, 0, 4);
				MPI.COMM_WORLD.Isend(countTwo, 0, countTwo.length, MPI.INT, 0, 5);

				

			} else {

				usrlist = new String[1024 * 1024];
				countOne = new int[1024 * 1024];
				tpclist = new String[1024 * 1024];
				countTwo = new int[1024 * 1024];

				for (int source = 1; source <= size - 1; source++) {
					MPI.COMM_WORLD.Recv(eachCount, 0, 1, MPI.INT, source, 1);
					count_term += eachCount[0];

					MPI.COMM_WORLD.Recv(usrlist, 0, usrlist.length, MPI.OBJECT, source, 2);

					MPI.COMM_WORLD.Recv(countOne, 0, countOne.length, MPI.INT, source, 3);
					MPI.COMM_WORLD.Recv(tpclist, 0, tpclist.length, MPI.OBJECT, source, 4);
					MPI.COMM_WORLD.Recv(countTwo, 0, countTwo.length, MPI.INT, source, 5);

					for (int m = 0; m < usrlist.length; m++) {
						if (mapUsers.containsKey(usrlist[m])) {
							mapUsers.put(usrlist[m], mapUsers.get(usrlist[m]) + countOne[m]);
						} else {
							mapUsers.put(usrlist[m], 1);
						}
					}
					for (int n = 0; n < tpclist.length; n++) {
						if (mapTopics.containsKey(tpclist[n])) {
							mapTopics.put(tpclist[n], mapTopics.get(tpclist[n]) + countTwo[n]);
						} else {
							mapTopics.put(tpclist[n], 1);
						}
					}

				}

				List<Map.Entry<String, Integer>> userList = new ArrayList<Map.Entry<String, Integer>>(
						mapUsers.entrySet());
				Collections.sort(userList, new Comparator<Map.Entry<String, Integer>>() {

					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {

						return o2.getValue().compareTo(o1.getValue());
					}
				});

				List<Map.Entry<String, Integer>> topicList = new ArrayList<Map.Entry<String, Integer>>(
						mapTopics.entrySet());
				Collections.sort(topicList, new Comparator<Map.Entry<String, Integer>>() {

					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {

						return o2.getValue().compareTo(o1.getValue());
					}
				});
				int t = 0;

				for (Map.Entry<String, Integer> mapping1 : userList) {
					if (t < 10)
						System.out.println(mapping1.getKey() + ":" + mapping1.getValue());
					t++;
				}

				t = 0;
				for (Map.Entry<String, Integer> mapping2 : topicList) {
					if (t < 10)
						System.out.println(mapping2.getKey() + ":" + mapping2.getValue());
					t++;
				}

				System.out.println("Your term \" " + term + " \" appears " + count_term + " times.");

				System.out.println("total time is " + (System.currentTimeMillis() - time)  + " msec");

			}
			}
			in.close();
			

			MPI.Finalize();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

}
