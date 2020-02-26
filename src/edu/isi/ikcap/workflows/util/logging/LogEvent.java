package edu.isi.ikcap.workflows.util.logging;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class LogEvent implements LoggingKeys {

    private static String START_POSTFIX = ".start";
    private static String END_POSTFIX = ".end";

    /** The variable that triggers generation of message id and the event id in the messages. */
    private static boolean _ADD_BLOAT = false;

    private String _eventName;
    private String _progName;
    private Map<String, String> _entityIdMap;
    private String _eventId;

    public LogEvent(String eventName, String programName, String entityType, String entityId) {

        _eventName = eventName;
        _progName = programName;
        _eventId = eventName + "_" + UUID.randomUUID().toString();

        _entityIdMap = new HashMap<String, String>();
        _entityIdMap.put(entityType, entityId);
    }

    public LogEvent(String eventName, String programName, Map<String, String> entityTypeToIdMap) {
        _eventName = eventName;
        _progName = programName;
        _eventId = eventName + "_" + UUID.randomUUID().toString();
        _entityIdMap = entityTypeToIdMap;
    }

    public LogEvent(String eventName, String programName) {
        _eventName = eventName;
        _progName = programName;
        _eventId = eventName + "_" + UUID.randomUUID().toString();
        _entityIdMap = new HashMap<String, String>();
    }

    public EventLogMessage createStartLogMsg() {
        String msgid = UUID.randomUUID().toString();
        EventLogMessage elm = new EventLogMessage(_eventName + START_POSTFIX);

        if (_ADD_BLOAT) {
            elm = elm.add(MSG_ID, msgid).add(EVENT_ID_KEY, _eventId);
        }
        elm.add(PROG, _progName);
        for (Map.Entry<String, String> entry : _entityIdMap.entrySet()) {
            elm.add(entry.getKey(), entry.getValue());
        }
        return elm;
    }

    public EventLogMessage createLogMsg() {
        String msgid = UUID.randomUUID().toString();
        EventLogMessage elm = new EventLogMessage(_eventName);

        if (_ADD_BLOAT) {
            elm = elm.add(MSG_ID, msgid).add(EVENT_ID_KEY, _eventId);
        }

        for (Map.Entry<String, String> entry : _entityIdMap.entrySet()) {
            elm.add(entry.getKey(), entry.getValue());
        }
        return elm;
    }

    public EventLogMessage createEndLogMsg() {
        String msgid = UUID.randomUUID().toString();
        EventLogMessage elm = new EventLogMessage(_eventName + END_POSTFIX);
        if (_ADD_BLOAT) {
            elm = elm.add(MSG_ID, msgid).add(EVENT_ID_KEY, _eventId);
        }
        for (Map.Entry<String, String> entry : _entityIdMap.entrySet()) {
            elm.add(entry.getKey(), entry.getValue());
        }
        return elm;
    }

    public static EventLogMessage createIdHierarchyLogMsg(
            String parentIdType, String parentId, String childIdType, Iterator<String> childIds) {
        String msgid = UUID.randomUUID().toString();
        String eventId = "idHierarchy_" + UUID.randomUUID().toString();
        EventLogMessage lm = new EventLogMessage("event.id.creation");
        if (_ADD_BLOAT) {
            lm = lm.add(MSG_ID, msgid).add(EVENT_ID_KEY, eventId);
        }
        lm.add("parent.id.type", parentIdType).add("parent.id", parentId);
        lm.add("child.ids.type", childIdType);
        StringBuffer cids = new StringBuffer("{");
        while (childIds.hasNext()) {
            cids.append(childIds.next());
            cids.append(",");
        }
        cids.append("}");
        lm.add("child.ids", cids.toString());
        return lm;
    }
}
