/* Copyright (c) 2019-2024, Tetsuo Kamina
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package signalj.timeseries;

import java.util.*;
import java.sql.*;
import java.lang.annotation.*;
import signalj.timeseries.annotation.*;

public class Synchronizer {
    private String dbName;
    private Connection conn;
    private int size = 0, delay = 0;
    private SignalClassInstance thisSignalClassInstance;
    private HashMap<PersistentSignal, String> map;
    private Vector<SignalClassInstance> externList; // upstreams;
    private Vector<Synchronizer> originalSources; // source signals;
    private HashMap<SignalClassInstance, Boolean> bufferlessMap;
    private HashMap<String, Queue<Timestamp>> bufferedMap;
    private Timestamp timeCursor = null, lastCheckPoint = null;
    private boolean union = true, bufferless = true;
    private int interval = -1, checkpointProceedCount = -1;
    private Vector<Synchronizer> sourceSyncs;
    private Vector<Thread> sourceThreads;
    //    private Synchronizer checkpointSynchronizer = null;
    private Map<Timestamp, Integer> waitListMap = new HashMap<Timestamp, Integer>();
    private List<Synchronizer> listeners = new ArrayList<Synchronizer>();

    public boolean listenFlag = false, listenCptFlag = false;
    private Thread checkpointTimer = null;

    public Synchronizer(String dbName, SignalClassInstance inst) {
	this.dbName = dbName;
	thisSignalClassInstance = inst;
	Class<?> clazz = thisSignalClassInstance.getClass();
	Annotation[] anns = clazz.getAnnotations();
	bufferlessMap = new HashMap<SignalClassInstance, Boolean>();
	bufferedMap = new HashMap<String, Queue<Timestamp>>();
	for (Annotation a : anns) {
	    if (a instanceof bufferless) {
		bufferless = true;
	    }
	    if (a instanceof buffered) {
		bufferless = false;
	    }
	    if (a instanceof mode) {
		if (((mode)a).value().equals("intersection")) {
		    union = true;
		} else {
		    union = false;
		}
	    }
	    if (a instanceof checkpointInterval) {
		int duration = ((checkpointInterval)a).value();
		//		System.out.println("getting @checkpointInterval: " + duration);
		if (duration < 0) continue;
		interval = duration;
	    }
	}
	map = new HashMap<PersistentSignal, String>();
	externList = new Vector<SignalClassInstance>();
	originalSources = new Vector<Synchronizer>();
	sourceSyncs = new Vector<Synchronizer>();
	sourceThreads = new Vector<Thread>();
    }

    public void setConnection(Connection conn) {
	this.conn = conn;
    }

    public void snapshot(Timestamp ts, boolean externFlag) {
	timeCursor = ts;
	if (externFlag) {
	    for (PersistentSignal ps : map.keySet()) {
		ps.snapshot(ts);
	    }
	}
    }

    public void add(PersistentSignal ps) {
	map.put(ps,null);
	size++;
	delay++;
    }

    class CheckPointTimer implements Runnable {
	private int interval;

	public CheckPointTimer(int interval) {
	    this.interval = interval;
	}
	
	public void run() {
	    while (true) {
		try {
		    Thread.sleep(interval);
		    System.out.println("recovery initiated by " + dbName);
		    pushCheckpointing();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	// starts the checkpointing. executed by sources.
	private void pushCheckpointing() {
	    try {
		PreparedStatement query = conn.prepareStatement("SELECT time FROM persistent_instances WHERE relname = ?");
		query.setString(1, dbName.toLowerCase());
		ResultSet rs = query.executeQuery();
		if (rs.next()) {
		    lastCheckPoint = rs.getTimestamp("time");
		}
		query.close();
		rs.close();
		
		PreparedStatement stmt = conn.prepareStatement("UPDATE persistent_instances SET time = now() WHERE relname = ?");
		stmt.setString(1, dbName.toLowerCase());
		stmt.execute();
		stmt.close();
		//		System.out.println("UPDATE on " + dbName);

		PreparedStatement stmt2 = conn.prepareStatement("DO $$BEGIN CREATE TRIGGER " + dbName.toLowerCase() + "_checkpointTr AFTER UPDATE OF time ON persistent_instances FOR EACH ROW WHEN (NEW.relname = '" + dbName.toLowerCase() + "') EXECUTE PROCEDURE notify_trigger('" + dbName.toLowerCase() + "_checkpoint'); EXCEPTION WHEN duplicate_object THEN null; END;$$");
		stmt2.execute();
		stmt2.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

    public Timestamp getLastCheckpoint() { return lastCheckPoint; }

    private void startTimer(int interval) {
	if (checkpointTimer == null) {
	    checkpointTimer = new Thread(new CheckPointTimer(interval*1000));
	    checkpointTimer.start();
	    sourceThreads.add(checkpointTimer);
	}
    }

    public boolean isSourceSync() {
	return sourceSyncs.size() == 0;
    }

    public synchronized void pushCheckPoint(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	System.out.println("externList.size() : " + externList.size());
	if (checkpointProceedCount < 0) checkpointProceedCount = externList.size() - 1;
	if (checkpointProceedCount == 0) {
	//	System.out.println("start checkpoint procedure on " + dbName);
	    thisSignalClassInstance.ensureConsistency(lastCheckPoint, currentCheckPoint);
	}
	checkpointProceedCount--;
    }

    public void addExtern(SignalClassInstance sci) {
	if (sci == null) return;
	
	if (!externList.contains(sci)) {
	    externList.add(sci);
	    sourceSyncs = getSourceSyncs(sci, new Vector<Synchronizer>());
	    if (interval > 0) {
		// all sources start the checkpoint timer
		for (Synchronizer s : sourceSyncs) {
		    //		    if (checkpointSynchronizer == null) {
		    //			checkpointSynchronizer = s;
		    s.startTimer(interval);
			//		    }
		    //		    break;
		}
	    }
	    if (union) {
		if (bufferless) {
		    bufferlessMap.put(sci,false);
		} else {
		    bufferedMap.put(sci.getID(), new LinkedList<Timestamp>());
		}
	    }
	    sci.addExternSync(this);
	}
    }

    private Vector<Synchronizer> getSourceSyncs(SignalClassInstance sci, Vector<Synchronizer> temp) {
	if (sci == null) return temp;
	
	Vector<SignalClassInstance> upstreams = sci.getSynchronizer().externList;
	if (upstreams.size() == 0) {
	    temp.add(sci.getSynchronizer());
	    return temp;
	}
	for (SignalClassInstance inst : upstreams) {
	    temp = getSourceSyncs(inst,temp);
	}
	return temp;
    }

    public Vector<Timestamp> getCheckPointTimestamps(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	Vector<Timestamp> status = new Vector<Timestamp>();
	try {
	    PreparedStatement statusQuery = conn.prepareStatement("SELECT time FROM " + dbName.toLowerCase() + " WHERE time >= ? AND time <= ?");
	    statusQuery.setTimestamp(1, lastCheckPoint);
	    statusQuery.setTimestamp(2, currentCheckPoint);
	    ResultSet statusRs = statusQuery.executeQuery();
	    while (statusRs.next()) {
		status.add(statusRs.getTimestamp("time"));
	    }
	    //	    System.out.println(status.size() + " rows in " + dbName + " from " + lastCheckPoint + " to " + currentCheckPoint);
	    statusRs.close();
	    statusQuery.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return status;
    }

    public Vector<Vector<Timestamp>> getUpstreamCheckPointTimestamps(Timestamp lastCheckPoint, Timestamp currentCheckPoint) {
	Vector<Vector<Timestamp>> retval = new Vector<Vector<Timestamp>>();
	for (SignalClassInstance sci : externList) {
	    Vector<Timestamp> upStamps = sci.getSynchronizer().getCheckPointTimestamps(lastCheckPoint, currentCheckPoint);
	    retval.add(upStamps);
	}
	return retval;
    }

    public void synchronize() {
	/*
	for (SignalClassInstance sci : externList) {
	    Vector<Timestamp> tsl = sci.timestampList();
	    for (Timestamp ts : tsl) {
		for (PersistentSignal ps : map.keySet()) {
		    if (ts.before(ps.latestTimestamp())) {
			sci.snapshot(ts);
			this.snapshot(ts, true);
			ps.pushSignalClassInst();
		    }
		}
	    }
	}
	*/
    }

    public void reval(SignalClassInstance exInst) {
	//	System.out.println("Synchronizer.reval()");
	//	System.out.println(getSignalClassInst().getID() + " Synchronizer.reval(): union: " + union + " bufferless: " + bufferless);
	if (union) {
	    Timestamp ts = exInst.latestTimestamp();
	    if (bufferless) {
		bufferlessMap.replace(exInst, true);
		/*
		for (boolean b: bufferlessMap.values()) {
		    if (!b) return;
		}
		*/
		for (SignalClassInstance inst : bufferlessMap.keySet()) {
		    if (inst.latestTimestamp().after(ts)) {
			ts = inst.latestTimestamp();
		    }
		}
		thisSignalClassInstance.snapshot(ts);
		snapshot(ts,false);
		for (PersistentSignal ps : map.keySet()) {
		    ps.pushSignalClassInst();
		    break;
		}
		bufferlessMap.replaceAll((k,v)-> false);
	    } else {
		bufferedMap.get(exInst.getID()).offer(exInst.latestTimestamp());
		for (Queue<Timestamp> q: bufferedMap.values()) {
		    if (q.peek() == null) {
			//			System.out.println("waiting...");
			return;
		    }
		}
		for (Map.Entry<String, Queue<Timestamp>> entry: bufferedMap.entrySet()) {
		    Timestamp instTs = entry.getValue().poll();
		    for (SignalClassInstance sci: externList) {
			if (sci.getID().equals(entry.getKey())) {
			    sci.snapshot(instTs);
			}
		    }
		    if (instTs.after(ts)) {
			ts = instTs;
		    }
		}
		for (PersistentSignal ps : map.keySet()) {
		    ps.pushSignalClassInst();
		}
	    }
	} else {
	    for (SignalClassInstance sci : externList) {
		sci.snapshot(new Timestamp(System.currentTimeMillis()));
	    }
	    for (PersistentSignal ps : map.keySet()) {
		ps.pushSignalClassInst();
	    }
	}
    }

    public void delay(PersistentSignal ps, String value) {
	if (map.get(ps) != null) return;

	map.put(ps, value);
	if (--delay == 0) {
	    StringBuffer stmt = new StringBuffer();
	    stmt.append("INSERT INTO " + dbName + "(time, ");
	    StringJoiner columns = new StringJoiner(",");
	    StringJoiner values = new StringJoiner(",");
	    for (PersistentSignal key : map.keySet()) {
		columns.add(key.name());
		values.add(map.get(key));
	    }
	    stmt.append(columns.toString());
	    String ts = timeCursor == null ? "NOW()" : "'" + timeCursor + "'";
	    stmt.append(") VALUES (" + ts + ", ");
	    stmt.append(values.toString());
	    stmt.append(")");
	    exec(stmt.toString());
	    for (PersistentSignal key : map.keySet()) {
		key.effect();
		map.put(key,null);
	    }
	    delay = size;
	}
    }

    private void exec(String query) {
	PreparedStatement stmt;
	try {
	    stmt = conn.prepareStatement(query);
	    stmt.executeUpdate();
	    stmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    public boolean getUnion() { return union; }

    public Synchronizer[] getSourceSyncs() {
	int size = externList.size();
	Synchronizer[] retval = new Synchronizer[size];
	for (int i=0; i<size; i++) retval[i] = externList.elementAt(i).getSynchronizer();
	return retval;
    }

    public void updateSourceSync(Synchronizer oldSync, Synchronizer newSync) {
	externList.remove(oldSync.getSignalClassInst());
	externList.add(newSync.getSignalClassInst());
    }

    public SignalClassInstance getSignalClassInst() {
	return thisSignalClassInstance;
    }

    public void blockSource() {
	for (Synchronizer s : sourceSyncs) {
	    if (s.isBlocked()) continue;
	    s.getSignalClassInst().block();
	}
    }

    public void unblockSource() {
	for (Synchronizer s : originalSources) {
	    s.getSignalClassInst().unblock();
	}
    }

    public List<Timestamp> queryImprocessingTimestamps(List<Timestamp> lst, Set<Synchronizer> visited) {
	long nanoT = System.nanoTime();
	Timestamp t = getImprocessingTimestamp();

	if (visited.contains(this)) {
	    return lst;
	}
	if (t != null && !lst.contains(t) && !isBlocked()) lst.add(t);
	visited.add(this);
	
	for (SignalClassInstance sci: externList) {
	    sci.getSynchronizer().queryImprocessingTimestamps(lst, visited);
	}

	for (PersistentSignal ps : map.keySet()) {
	    for (Object s : ps.getDownstreams()) {
		((Synchronizer)s).queryImprocessingTimestamps(lst, visited);
	    }
	    break;
	}
	
	return lst;
    }

    private Timestamp getImprocessingTimestamp() {
	for (PersistentSignal ps : map.keySet()) {
	    Timestamp t = ps.getImprocessingTimestamp();
	    if (t != null) return t;
	}
	return null;
    }

    private boolean isBlocked() {
	for (PersistentSignal ps : map.keySet()) {
	    if (ps.isBlocked()) return true;
	}
	return false;
    }

    public List<Synchronizer> queryMostDownstreams(List<Synchronizer> lst, Set<Synchronizer> visited) {
	if(visited.contains(this)) {
	    return lst;
	}
	visited.add(this);
	for (PersistentSignal ps : map.keySet()) {
	    Vector<Synchronizer> down = ps.getDownstreams();
	    if (down.isEmpty()) {
		lst.add(this);
	    }
	    for (Synchronizer s : down) {
		s.queryMostDownstreams(lst, visited);
	    }
	    break;
	}
	for (SignalClassInstance sci : externList) {
	    sci.getSynchronizer().queryMostDownstreams(lst, visited);
	}
	
	return lst;
    }

    public void initWaitListMap() {
	waitListMap = new HashMap<Timestamp, Integer>();
    }

    public void putWaitListMap(Timestamp t, int i) {
	waitListMap.put(t, i);
    }

    public int getWaitListMap(Timestamp t) {
	return waitListMap.get(t);
    }

    public Set<Timestamp> waitListMapKeySet() {
	return waitListMap.keySet();
    }

    public Timestamp latestTimestamp() {
	return thisSignalClassInstance.latestTimestamp();
    }

    public List<Synchronizer> listeners() {
	return listeners;
    }

    public void addListener(Synchronizer s) {
	listeners.add(s);
    }

    public int countWaits(Timestamp t) {
	return 0; // TODO
    }

    public Vector<Synchronizer> getOriginalSources() {
	return originalSources;
    }

    public void mergeSources(Vector<Synchronizer> sources) {
	for (Synchronizer s : sources) {
	    if (!originalSources.contains(s))
		originalSources.add(s);
	}
    }

    public void propagateMerge() {
	for (SignalClassInstance i : externList) {
	    Synchronizer s = i.getSynchronizer();
	    s.mergeSources(originalSources);
	    s.propagateMerge();
	}
    }

    public void updateSource() {
	for (SignalClassInstance i : externList) {
	    Vector<Synchronizer> sources = i.getSynchronizer().getOriginalSources();
	    if (sources.size() == 0) {
		sources.add(i.getSynchronizer());
	    }
	    mergeSources(sources);
	    propagateMerge();
	}
    }
}
