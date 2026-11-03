package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpJsonRepresentation;

/** Writes a failure of the consumers to the error output, which the process manager keeps in its log. */
class CcpPubSubLogFailure implements CcpBusiness {

	/** The single instance: the business keeps no state. */
	static final CcpPubSubLogFailure INSTANCE = new CcpPubSubLogFailure();

	private CcpPubSubLogFailure() {}

	/**
	 * Writes the failure.
	 * @param json the details of the failure
	 * @return the details of the failure
	 */
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		String failure = json.asPrettyJson();
		System.err.println(failure);
		return json;
	}
}
