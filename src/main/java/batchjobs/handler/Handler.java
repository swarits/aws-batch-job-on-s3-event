package batchjobs.handler;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handler implements RequestHandler<S3Event, String> {
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger logger = LoggerFactory.getLogger(Handler.class);
	private static final String JOB_PARAM_FILE_NAME = "fileName";
	private static final String ENV_VAR_QUEUE = "JOB_QUEUE";
	private static final String ENV_VAR_DEFINITION = "JOB_DEFINITION";
	private static final String ENV_VAR_JOB_NAME = "JOB_NAME";

	@Override
	public String handleRequest(S3Event s3event, Context context) {

		logger.info("EVENT: " + gson.toJson(s3event));
		S3EventNotificationRecord record = s3event.getRecords().get(0);

		String srcBucket = record.getS3().getBucket().getName();
		String fileKey = record.getS3().getObject().getUrlDecodedKey();

		String jobName = System.getenv(ENV_VAR_JOB_NAME);
		String jobQueue = System.getenv(ENV_VAR_QUEUE);
		String jobDef = System.getenv(ENV_VAR_DEFINITION);

		// submit job to aws batch to process file
		try {
			logger.info("Submitting Batch Job...");
			AWSBatch client = AWSBatchClientBuilder.standard().withRegion("ap-south-1").build();
			Map<String, String> params = new HashMap<>();
			params.put(JOB_PARAM_FILE_NAME, fileKey);
			SubmitJobRequest request = new SubmitJobRequest().withJobName(jobName).withJobQueue(jobQueue)
					.withJobDefinition(jobDef).withParameters(params);
			logger.info("Job name:" + jobName + " Job Queue: " + jobQueue + " Job Definition: " + jobDef);
			SubmitJobResult response = client.submitJob(request);
		} catch (AmazonServiceException e) {
			logger.error(e.getErrorMessage());
			System.exit(1);
		}

		logger.info("Successfully submitted batch job for " + fileKey);
		return "Ok";
	}
}
