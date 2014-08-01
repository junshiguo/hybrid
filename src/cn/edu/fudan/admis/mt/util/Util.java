package cn.edu.fudan.admis.mt.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util
{
	static final Logger logger = LogManager.getLogger(Util.class);

	public static <K, V extends Comparable<? super V>> List<Entry<K, V>> sortByValue(
			Map<K, V> map, final boolean asc)
	{
		List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
		Collections.sort(list, new Comparator<Entry<K, V>>()
		{
			@Override
			public int compare(Entry<K, V> e1, Entry<K, V> e2)
			{
				if (asc)
					return e1.getValue().compareTo(e2.getValue());
				else
					return e2.getValue().compareTo(e1.getValue());
			}
		});
		logger.debug(list);
		return list;
	}

	public static <K, V> Queue<K> getKey(List<Entry<K, V>> list)
	{
		Queue<K> result = new LinkedList<>();
		for (Entry<K, V> entry : list)
		{
			result.offer(entry.getKey());
		}
		return result;
	}
}
