package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpStringDecorator;

/**
 * Appends one line of JSON per processed message (topic, result, duration, wait in the queue, message id and the error,
 * when there is one) to {@code logs/mensagens.jsonl}, relative to the working directory of the consumers
 * ({@code /opt/jobsnow} on the VM). The Ops Agent sends each line to Cloud Logging as structured fields (log
 * {@code mensagens}), which the local panel {@code 31-mensagens-das-filas.py} reads. The threads of every subscription
 * share the file, so the writes are serialized; a failure to write is only reported on the error output, never breaks the
 * acknowledgement of the message.
 */
class CcpPubSubWriteMessageLog implements CcpBusiness {

	/** The texts of this class. */
	enum Constants implements CcpJsonFieldName {
		/** The file of the lines, relative to the working directory. */
		FILE_PATH { public String getValue() { return "logs/mensagens.jsonl"; } }
	}

	/** The single instance: every thread writes through it. */
	static final CcpPubSubWriteMessageLog INSTANCE = new CcpPubSubWriteMessageLog();

	/** The file of the lines. */
	private final CcpFileDecorator file;

	private CcpPubSubWriteMessageLog() {
		String filePath = Constants.FILE_PATH.getValue();
		CcpStringDecorator filePathDecorator = new CcpStringDecorator(filePath);
		this.file = filePathDecorator.file();
	}

	/**
	 * Writes the line of one message.
	 * @param json the fields of the line
	 * @return the fields of the line
	 */
	public synchronized CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		try {
			String line = json.asUgglyJson();
			this.file.append(line);
			return json;
		} catch (Throwable e) {
			e.printStackTrace();
			return json;
		}
	}
}
