package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;

/**
 * Runs each message of a consumer that names its own {@code CcpMensageriaSender} (column {@code senderClass} of
 * {@code topicos.tsv}, since 2026-11-05): while the message runs, only its thread sees that sender
 * ({@link CcpDependencyInjection#executeWithDependencyOfThisThread}), and the consumers of the other queues, in the same
 * JVM, keep the synchronous sender of the registry.
 */
class CcpPubSubTaskWithOwnSender implements CcpBusiness {

	/** Runs the message. */
	private final CcpBusiness task;

	/** The sender of this consumer. */
	private final CcpMensageriaSender sender;

	/**
	 * Keeps the task and the sender.
	 * @param task runs the message
	 * @param sender the sender of this consumer
	 */
	CcpPubSubTaskWithOwnSender(CcpBusiness task, CcpMensageriaSender sender) {
		this.task = task;
		this.sender = sender;
	}

	/**
	 * Runs the message seeing the sender of this consumer.
	 * @param json the message
	 * @return what the task returns
	 */
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		CcpJsonRepresentation result = CcpDependencyInjection.executeWithDependencyOfThisThread(json, this.task, this.sender);
		return result;
	}
}
