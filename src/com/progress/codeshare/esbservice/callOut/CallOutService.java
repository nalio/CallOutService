package com.progress.codeshare.esbservice.callOut;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import com.sonicsw.xq.*;
import com.sonicsw.xq.service.sj.MessageUtils;

public class CallOutService implements XQService {

	//	 This is the XQLog (the container's logging mechanism).
	private static XQLog m_xqLog = null;

	// This is the the log prefix that helps identify this service during
	// logging
	private static String m_logPrefix = "";

	// These hold version information.
	private static int s_major = 1;

	private static int s_minor = 0;

	private static int s_buildNumber = 0;

    private static final String OUTPUT_MESSAGE = "Message";

	private static final String OUTPUT_MESSAGE_PART = "Message Part";

	private static final String PARAM_NAME_CALLED_ENDPOINTS = "calledEndpoints";

	private static final String PARAM_NAME_KEEP_ORIGINAL_PART = "keepOriginalPart";

	private static final String PARAM_NAME_MESSAGE_PART = "messagePart";

	private static final String PARAM_NAME_OUTPUT = "output";

	private static final String PARAM_NAME_TIMEOUT = "timeout";

	private static final Pattern PATTERN = Pattern.compile("([^,]+),?");
	
    /**
     * Constructor for a CallOutService
     */
	public CallOutService () {	
	}
	
	private void callOutServiceContext(XQServiceContext ctx) throws XQServiceException
	{
		try {
			final XQParameters params = ctx.getParameters();

			final String output = params.getParameter(PARAM_NAME_OUTPUT,
					XQConstants.PARAM_STRING);

			final int messagePart = params.getIntParameter(
					PARAM_NAME_MESSAGE_PART, XQConstants.PARAM_STRING);

			final String calledEndpoints = params.getParameter(
					PARAM_NAME_CALLED_ENDPOINTS, XQConstants.PARAM_STRING);

			final Matcher matcher = PATTERN.matcher(calledEndpoints);

			final List<XQEndpoint> endpointList = new ArrayList<XQEndpoint>();

			final XQEndpointManager manager = ctx.getEndpointManager();

			while (matcher.find())
				endpointList.add(manager.getEndpoint(matcher.group(1).trim()));

			final ListIterator endpointIterator = endpointList.listIterator();

			final XQMessageFactory msgFactory = ctx.getMessageFactory();

			final int timeout = params.getIntParameter(PARAM_NAME_TIMEOUT,
					XQConstants.PARAM_STRING);

			final boolean keepOriginalPart = params.getBooleanParameter(
					PARAM_NAME_KEEP_ORIGINAL_PART, XQConstants.PARAM_STRING);

			if (output.equals(OUTPUT_MESSAGE)) {

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {

							while (endpointIterator.hasNext()) {
								final XQMessage newMsg = msgFactory
										.createMessage();

								/*
								 * Copy all headers from the original message to
								 * the new message
								 */
								MessageUtils.copyAllHeaders(origMsg, newMsg);

								final XQPart origPart = origMsg.getPart(i);

								/*
								 * Decide whether to keep the original part or
								 * not
								 */
								if (keepOriginalPart) {
									origPart.setContentId("original_part_" + i);

									newMsg.addPart(origPart);
								}

								final XQMessage reqMsg = msgFactory
										.createMessage();

								/*
								 * Copy all headers of the original part to the
								 * request message
								 */
								final XQHeader header = origPart.getHeader();

								final Iterator keyIterator = header.getKeys();

								while (keyIterator.hasNext()) {
									final String key = (String) keyIterator
											.next();

									reqMsg.setHeaderValue(key, header
											.getValue(key));
								}

								final XQPart reqPart = reqMsg.createPart();

								reqPart.setContent(origPart.getContent(),
										origPart.getContentType());

								reqMsg.addPart(reqPart);

								final XQEndpoint endpoint = (XQEndpoint) endpointIterator
										.next();

								final XQMessage resMsg = endpoint.call(reqMsg,
										timeout);

								/*
								 * Copy all parts of the response message to the
								 * new message
								 */
								for (int j = 0; j < resMsg.getPartCount(); j++) {
									final XQPart newPart = resMsg.getPart(i);

									newPart.setContentId("Result-"
											+ endpointIterator.previousIndex()
											+ "_" + i + "_" + j);

									newMsg.addPart(newPart);
								}

								env.setMessage(newMsg);

								if (addressIterator.hasNext())
									ctx.addOutgoing(env);

							}

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			} else if (output.equals(OUTPUT_MESSAGE_PART)) {

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage origMsg = env.getMessage();

					final Iterator addressIterator = env.getAddresses();

					for (int i = 0; i < origMsg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							final XQPart origPart = origMsg.getPart(i);

							/* Decide whether to keep the original part or not */
							if (keepOriginalPart) {
								origPart.setContentId("original_part_" + i);

								newMsg.addPart(origPart);
							}

							final XQMessage reqMsg = msgFactory.createMessage();

							/*
							 * Copy all headers of the original part to the
							 * request message
							 */
							final XQHeader header = origPart.getHeader();

							final Iterator keyIterator = header.getKeys();

							while (keyIterator.hasNext()) {
								final String key = (String) keyIterator.next();

								reqMsg
										.setHeaderValue(key, header
												.getValue(key));
							}

							final XQPart reqPart = reqMsg.createPart();

							reqPart.setContent(origPart.getContent(), origPart
									.getContentType());

							reqMsg.addPart(reqPart);

							while (endpointIterator.hasNext()) {
								final XQEndpoint endpoint = (XQEndpoint) endpointIterator
										.next();

								final XQMessage resMsg = endpoint.call(reqMsg,
										timeout);

								/*
								 * Copy all parts of the response message to the
								 * new message
								 */
								for (int j = 0; j < resMsg.getPartCount(); j++) {
									final XQPart newPart = resMsg.getPart(i);

									newPart.setContentId("Result-"
											+ endpointIterator.previousIndex()
											+ "_" + i + "_" + j);

									newMsg.addPart(newPart);
								}

							}

							env.setMessage(newMsg);

							if (addressIterator.hasNext())
								ctx.addOutgoing(env);

						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}
		
	}
	
	/**
     * Initialize the XQService by processing its initialization parameters.
     *
     * <p> This method implements a required XQService method.
     *
     * @param initialContext The Initial Service Context provides access to:<br>
     * <ul>
     * <li>The configuration parameters for this instance of the CallOutService.</li>
     * <li>The XQLog for this instance of the CallOutService.</li>
     * </ul>
     * @exception XQServiceException Used in the event of some error.
     */
    public void init(XQInitContext initialContext) throws XQServiceException {
		XQParameters params = initialContext.getParameters();
		m_xqLog = initialContext.getLog();
		setLogPrefix(params);

		m_xqLog.logInformation(m_logPrefix + " Initializing ...");

		writeStartupMessage(params);
		writeParameters(params);
		// perform initilization work.

		m_xqLog.logInformation(m_logPrefix + " Initialized ...");
    }


	/**
	 * Handle the arrival of XQMessages in the INBOX.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 * 
	 * @param ctx
	 *            The service context.
	 * @exception XQServiceException
	 *                Thrown in the event of a processing error.
	 */
	public void service(XQServiceContext ctx) throws XQServiceException {

		if (ctx == null)
			throw new XQServiceException("Service Context cannot be null.");
		else {
			callOutServiceContext(ctx);
		}
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 */
	public void destroy() {
		m_xqLog.logInformation(m_logPrefix + "Destroying...");

		m_xqLog.logInformation(m_logPrefix + "Destroyed...");
	}

	/**
	 * Called by the container on container start.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void start() {
		m_xqLog.logInformation(m_logPrefix + "Starting...");

		m_xqLog.logInformation(m_logPrefix + "Started...");
	}

	/**
	 * Called by the container on container stop.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void stop() {
		m_xqLog.logInformation(m_logPrefix + "Stopping...");

		m_xqLog.logInformation(m_logPrefix + "Stopped...");
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 */
	protected void setLogPrefix(XQParameters params) {
		String serviceName = params.getParameter(
				XQConstants.PARAM_SERVICE_NAME, XQConstants.PARAM_STRING);
		m_logPrefix = "[ " + serviceName + " ]";
	}

	/**
	 * Provide access to the service implemented version.
	 * 
	 */
	protected String getVersion() {
		return s_major + "." + s_minor + ". build " + s_buildNumber;
	}

	/**
	 * Writes a standard service startup message to the log.
	 */
	protected void writeStartupMessage(XQParameters params) {
		
		final StringBuffer buffer = new StringBuffer();
		
		String serviceTypeName = params.getParameter(
				XQConstants.SERVICE_PARAM_SERVICE_TYPE,
				XQConstants.PARAM_STRING);
		
		buffer.append("\n\n");
		buffer.append("\t\t " + serviceTypeName + "\n ");
		
		buffer.append("\t\t Version ");
		buffer.append(" " + getVersion());
		buffer.append("\n");
				
		buffer.append("\t\t Copyright (c) 2008, Progress Sonic Software Corporation.");
		buffer.append("\n");
		
		buffer.append("\t\t All rights reserved. ");
		buffer.append("\n");
		
		m_xqLog.logInformation(buffer.toString());
	}

	/**
	 * Writes parameters to log.
	 */
	protected void writeParameters(XQParameters params) {

		final Map map = params.getAllInfo();
		final Iterator iter = map.values().iterator();

		while (iter.hasNext()) {
			final XQParameterInfo info = (XQParameterInfo) iter.next();

			if (info.getType() == XQConstants.PARAM_XML) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Name =  "
						+ info.getName());
			} else if (info.getType() == XQConstants.PARAM_STRING) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Name = "
						+ info.getName());
			}

			if (info.getRef() != null) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Reference = "
						+ info.getRef());

				// If this is too verbose
				// /then a simple change from logInformation to logDebug
				// will ensure file content is not displayed
				// unless the logging level is set to debug for the ESB
				// Container.
				m_xqLog.logDebug(m_logPrefix
						+ "----Parameter Value Start--------");
				m_xqLog.logDebug("\n" + info.getValue() + "\n");
				m_xqLog.logDebug(m_logPrefix
						+ "----Parameter Value End--------");
			} else {
				m_xqLog.logDebug(m_logPrefix + "Parameter Value = "
						+ info.getValue());
			}
		}
	}

}
