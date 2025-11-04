import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CalculateAveragelathan1623 {

	private static final String FILE = "./measurements.txt";

	private static final int THREAD_COUNT = 8;

	private static class MeasurementAggregator {
		private double min = Double.POSITIVE_INFINITY;
		private double max = Double.NEGATIVE_INFINITY;
		private double sum;
		private long count;

		private static MeasurementAggregator create(double value) {
			var ma = new MeasurementAggregator();
			ma.min = value;
			ma.max = value;
			ma.sum += value;
			ma.count++;
			return ma;
		}

		private static MeasurementAggregator combine(MeasurementAggregator a, MeasurementAggregator b) {
			MeasurementAggregator combined = new MeasurementAggregator();
			combined.min = Math.min(a.min, b.min);
			combined.max = Math.max(a.max, b.max);
			combined.sum = a.sum + b.sum;
			combined.count = a.count + b.count;
			return combined;
		}
	}

	private static record ResultRow(double min, double mean, double max) {
		public String toString() {
			return round(min) + "/" + round(mean) + "/" + round(max);
		}

		private double round(double value) {
			return Math.round(value * 10.0) / 10.0;
		}
	};

	private static record Chunk(MappedByteBuffer buffer, long start, long end) {
	}

	public static void main(String[] args) throws InterruptedException {
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

			var combinedMeasurementMap = new ConcurrentHashMap<String, MeasurementAggregator>();
			var chunks = chunk_file();
			System.out.println("File chunked");
			var futures = new CompletableFuture[THREAD_COUNT];

			for (int i = 0; i < THREAD_COUNT; i++) {
				final var index = i;
				futures[i] = CompletableFuture
						.supplyAsync(() -> {
							return process_chunk(chunks[index]);
						}, executor)
						.whenComplete((result, ex) -> {
							if (null != ex) {
								System.out.println(ex);
							} else {
								result.forEach((key, value) -> {
									combinedMeasurementMap.merge(key, value, MeasurementAggregator::combine);
								});
							}
						});
			}
			CompletableFuture.allOf(futures).join();

			var output = combinedMeasurementMap
					.entrySet()
					.stream()
					.sorted(Map.Entry.comparingByKey())
					.map(entry -> {
						var val = entry.getValue();
						var result = new ResultRow(val.min, (val.sum / val.count), val.max);
						return entry.getKey() + "=" + result;
					})
					.collect(Collectors.joining(", "));
			System.out.println("{" + output + "}");

		} catch (IOException e) {
			System.out.println(e);
		}
	}

	/**
	 * Reads file chunks into memory which are created based on the THREAD_COUNT
	 **/
	private static Chunk[] chunk_file() throws IOException {
		try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
			long fileSize = channel.size();
			var mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

			long[] boundaries = new long[THREAD_COUNT + 1];
			boundaries[0] = 0;
			boundaries[THREAD_COUNT] = fileSize;

			long chunkSize = fileSize / THREAD_COUNT;
			for (int i = 1; i < THREAD_COUNT; i++) {
				long pos = chunkSize * i;
				while (pos < fileSize && mapped.get((int) pos) != '\n') {
					pos++;
				}
				boundaries[i] = pos + 1;
			}

			var chunks = new Chunk[THREAD_COUNT];
			for (int i = 0; i < THREAD_COUNT; i++) {
				chunks[i] = new Chunk(mapped, boundaries[i], boundaries[i + 1]);
			}

			return chunks;
		}
	}

	/**
	 * Processes a chunk in the file to calculate measurement data for each station
	 **/
	private static HashMap<String, MeasurementAggregator> process_chunk(Chunk chunk) {
		var measurementMap = new HashMap<String, MeasurementAggregator>();
		var splitIndex = 0;
		var lineStart = 0;

		//TODO optimize, this is hot loop
		for (int i = 0; i < chunk.end; i++) {
			byte b = chunk.buffer.get(i);
			if (b == ';') {
				splitIndex = i;
				continue;
			}
			if (b == '\n') {
				int stationLen = splitIndex - lineStart;
				//TODO its slow to get bytes into a string here, maybe new hashmap impl so i dont need string
				byte[] stationBytes = new byte[stationLen];
				chunk.buffer.get(lineStart, stationBytes, 0, stationLen);
				var station = new String(stationBytes);
				double value = parseDouble(chunk.buffer, splitIndex + 1, i);
				measurementMap.merge(station, MeasurementAggregator.create(value), MeasurementAggregator::combine);
				lineStart = i + 1;
			}
		}

		return measurementMap;
	}
	
	/**
	 * Helper to parse double directly from MappedByteBuffer region
	 **/
	private static double parseDouble(MappedByteBuffer buffer, int start, int end) {
		boolean negative = false;
		int pos = start;

		if (buffer.get(pos) == '-') {
			negative = true;
			pos++;
		}

		int intPart = 0;
		while (pos < end && buffer.get(pos) != '.') {
			intPart = intPart * 10 + (buffer.get(pos) - '0');
			pos++;
		}

		int decimalPart = 0;
		int decimalPlaces = 0;
		if (pos < end && buffer.get(pos) == '.') {
			pos++;
			while (pos < end) {
				decimalPart = decimalPart * 10 + (buffer.get(pos) - '0');
				decimalPlaces++;
				pos++;
			}
		}

		double result = intPart + decimalPart / Math.pow(10, decimalPlaces);
		return negative ? -result : result;
	}
}
