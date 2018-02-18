
package info.debatty.java.datasets.enron;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.util.MimeMessageParser;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Email implements Serializable {
	private final String raw;
	private String mailbox;
	private String user;
	private final String from;
	private final List<String> to;
	private List<String> cc;
	private List<String> bcc;
	private final String message_id;
	private final String subject;
	private final String content;
	private Date receivedDate;
	private final boolean fromOutsideOrg;

	private Logger logger = Logger.getLogger(this.getClass());

	Email(final String raw, final String mailbox) throws MessagingException, Exception {
		this.raw = raw;
		String[] strings = mailbox.split("\\\\", 2);
		if (strings.length == 2) {
			this.user = strings[0];
			this.mailbox = strings[1];
		}else {
			System.out.println("mailbox :"+ mailbox);
			System.out.println("raw:"+ raw);
		}

		Session s = Session.getDefaultInstance(new Properties());
		InputStream is = new ByteArrayInputStream(raw.getBytes());
		MimeMessage message = new MimeMessage(s, is);
		MimeMessageParser parser = new MimeMessageParser(message);

		from = parser.getFrom();
		to = addressToString(parser.getTo());
		subject = parser.getSubject();
		content = parser.getPlainContent();
		MimeMessage mimeMsg = parser.getMimeMessage();
		message_id = mimeMsg.getMessageID();
		String date = (mimeMsg.getHeader("Date"))[0];
		// mimeMsg.getc
		if (StringUtils.isNotEmpty(date)) {
			if (date.contains(" -")) {
				date = date.substring(0, date.indexOf(" -"));
			}
			try {
				receivedDate = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:SS").parse(date);
			} catch (Exception e) {
				logger.error("Error while parsing received date with message id : " + message_id, e);
			}
		}

		fromOutsideOrg = !from.endsWith("@enron.com");

		String[] ccL = mimeMsg.getHeader("X-cc");
		String[] bccL = mimeMsg.getHeader("X-bcc");
		if (ccL != null && ccL.length > 0) {
			cc = addressToString(parser.getCc());
		}
		if (bccL != null && bccL.length > 0) {
			bcc = addressToString(parser.getBcc());
		}

	}

	public String getUser() {
		return user;
	}

	public String getFrom() throws Exception {
		return from;
	}

	public String getMailbox() {
		return mailbox;
	}

	private static List<String> addressToString(List<Address> addresses) {
		ArrayList<String> strings = new ArrayList<String>();
		for (Address address : addresses) {
			strings.add(address.toString());
		}
		return strings;
	}

	public List<String> getTo() throws Exception {
		return to;
	}

	public List<String> getCc() {
		return cc;
	}

	public String getMessageID() {
		return message_id;
	}

	public List<String> getBcc() {
		return bcc;
	}

	public String getContent() {
		return content;
	}

	public String getSubject() {
		return subject;
	}

	public String getRaw() {
		return raw;
	}

	@Override
	public boolean equals(Object other) {

		Email other_email = (Email) other;
		return other_email.message_id.equals(this.message_id);

	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 17 * hash + (this.message_id != null ? this.message_id.hashCode() : 0);
		return hash;
	}

	public DBObject getDBObject() {
		DBObject dbObj = new BasicDBObject();
		dbObj.put("raw", raw);
		dbObj.put("mailbox", mailbox);
		dbObj.put("user", user);
		dbObj.put("from", from);
		BasicDBList toList = new BasicDBList();
		toList.addAll(to);
		dbObj.put("to", toList);
		BasicDBList ccList = new BasicDBList();
		ccList.addAll(cc);
		dbObj.put("cc", ccList);
		BasicDBList bccList = new BasicDBList();
		bccList.addAll(bcc);
		dbObj.put("bcc", bccList);
		dbObj.put("message_id", message_id);
		dbObj.put("subject", subject);
		dbObj.put("content", content);
		dbObj.put("received_date", receivedDate);

		if (!from.endsWith("@enron.com")) {
			dbObj.put("", false);
		}

		return dbObj;
	}

	public Document getDoc() {
		Document doc = new Document();
		doc.put("raw", raw);
		doc.put("mailbox", mailbox);
		doc.put("user", user);
		doc.put("from", from);
		if (to != null && to.size() > 0) {
			BasicDBList toList = new BasicDBList();
			toList.addAll(to);
			doc.put("to", toList);
		}
		if (cc != null && cc.size() > 0) {
			BasicDBList ccList = new BasicDBList();
			ccList.addAll(cc);
			doc.put("cc", ccList);
		}
		if (bcc != null && bcc.size() > 0) {
			BasicDBList bccList = new BasicDBList();
			bccList.addAll(bcc);
			doc.put("bcc", bccList);
		}
		doc.put("message_id", message_id);
		doc.put("subject", subject);
		doc.put("content", content);
		doc.put("received_date", receivedDate);

		if (!from.endsWith("@enron.com")) {
			doc.put("fromOutsider", false);
		}

		return doc;
	}

}