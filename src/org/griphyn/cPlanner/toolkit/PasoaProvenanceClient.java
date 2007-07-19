package org.griphyn.cPlanner.toolkit;


import java.io.BufferedReader;
import java.io.StringReader;
import java.io.FileReader;
import java.net.URL;
import java.util.LinkedList;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.pasoa.common.Constants;
import org.pasoa.pstructure.Record;
import org.pasoa.pstructure.ActorStatePAssertion;
import org.pasoa.pstructure.GlobalPAssertionKey;
import org.pasoa.pstructure.InteractionKey;
import org.pasoa.pstructure.InteractionPAssertion;
import org.pasoa.pstructure.ObjectID;
import org.pasoa.pstructure.RelationshipPAssertion;
import org.pasoa.storeclient.ClientLib;
import org.pasoa.util.httpsoap.WSAddressEndpoint;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import org.xml.sax.InputSource;
import java.io.StringWriter;
import java.io.IOException;
import java.io.Reader;
import org.xml.sax.InputSource;
public class PasoaProvenanceClient {

    /** change this to connect to the preserv server **/
    public static String URL = "http://localhost:8080/preserv-1.0";
    public static String XMLHEADER ="<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>";
    public static String CONDOR= "www.cs.wisc.edu/condor";
    public long filecount=0;
    public static String documentationStyle = "http://www.pasoa.org/schemas/pegasus";
    public   ClientLib clientLib = null;
    public	URL provenanceStore = null;
    public String jobname=null;
    public String wf_label=null;
    public String wf_planned_time=null;
    public String transformation=null;
    public Element docelement=null;
    public Element daxelement=null;
    //  public List input=null;
    //   public List output=null;
    public List parents=null;
    public List children=null;
    public Map input = null;
    public Map output = null;


    public PasoaProvenanceClient(String url){
        clientLib=new ClientLib();
        try{
            provenanceStore = new URL(url + "/record");
        }catch(Exception e){
            System.err.println("Bad Bad Bad url");
        }
    }

    public PasoaProvenanceClient(){
        clientLib=new ClientLib();
        try{
            provenanceStore = new URL(URL + "/record");
        }catch(Exception e){
            System.err.println("Bad Bad Bad url");
        }

    }



    public static void main(String[] args) throws Exception {

        PasoaProvenanceClient cle=null;
        String jobfile=null;
        String daxfile=null;
        String dagfile=null;
        String url=null;
        if(args.length<3){
            System.err.println("Usage: Client daxfile dagfile outfile");
           // System.err.println("Usage: Client daxfile dagfile preservurl");
            System.exit(1);

        }else if(args.length==3){
            jobfile=args[2];
            daxfile=args[0];
            dagfile=args[1];
            cle = new PasoaProvenanceClient();

        }
	/*}else {
	    jobfile=args[0];
	    daxfile=args[0];
	    dagfile=args[2];
	    url=args[3];
            cle = new PasoaProvenanceClient(url);

	}*/
	try{
	   	    cle.jobname=(new File(jobfile)).getName().split("\\.out")[0];
	    System.out.println("Processing job --- "+ cle.jobname);
	    cle.parseKickstartRecord(jobfile);
            cle.parseDag(dagfile);
            List newlist=new ArrayList();
            if(cle.parents!=null && !cle.parents.isEmpty()){
                System.out.println("Adding parents "+ cle.parents);
                newlist.addAll(cle.parents);
            }
            if(cle.children!=null && !cle.children.isEmpty()){
                System.out.println("Adding children  "+ cle.children);
                newlist.addAll(cle.children);
            }
            System.out.println("Adding job "+ cle.jobname);

            newlist.add(cle.jobname);
            System.out.println("Job List is  "+ newlist);
            cle.parseFiles(newlist);
//            cle.parseDaxFile(daxfile,newlist);
 //           cle.parseInput();
            System.out.println("Inputs == "+cle.input);
            System.out.println("Outputs == "+cle.output);

            if(cle.jobname.startsWith("rc_tx")|| (cle.jobname.startsWith("new_rc_tx"))){
                InteractionKey ik = cle.transferInvocationInteraction();
                cle.transferCompletionInteraction(ik);
            } else if(cle.jobname.startsWith("new_rc_register")){
                InteractionKey ik = cle.registerInvocationInteraction();
                cle.registerCompletionInteraction(ik);
            } else if(cle.jobname.endsWith("cdir")) {
                //write this handler
            } else if(cle.jobname.startsWith("cln_")){
                //write this handler
            }else if(cle.jobname.endsWith("concat")){
                //write this handler
            }else{
                InteractionKey ik = cle.jobInvocationInteraction();
                cle.jobCompletionInteraction(ik);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private void parseDag(String file) throws Exception{
        BufferedReader bf = new BufferedReader(new FileReader(file));
        String line = null;
        while((line=bf.readLine())!=null){
            String[] list = null;
            if (line.startsWith("PARENT")){
                list = line.split(" ");
            }
            if(list!=null){
                if(list[1].equalsIgnoreCase(jobname)){
                    if(children==null){
                        children=new ArrayList();
                    }
                    children.add(list[3]);
                }
                if(list[3].equalsIgnoreCase(jobname)){
                    if(parents==null){
                        parents=new ArrayList();
                    }
                    parents.add(list[1]);
                }
            }
        }
        bf.close();
    }

    private void parseKickstartRecord(String file) throws Exception{
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        List records=extractToMemory(new File(file));
        if(records!=null){
            for (Iterator i=records.iterator();i.hasNext();){

                Document msgDoc = db.parse(new InputSource(new StringReader((String)i.next())));
                docelement = msgDoc.getDocumentElement();
                transformation = docelement.getAttribute("transformation");
                wf_label=docelement.getAttribute("wf-label");
                wf_planned_time=docelement.getAttribute("wf-stamp");

            }
        }
    }

    public List extractToMemory( java.io.File input )
      throws Exception
    {
        List result = new ArrayList();
           StringWriter out = null;
           // open the files
           int p1, p2, state = 0;
           try {
             BufferedReader in = new BufferedReader( new FileReader(input) );
             out = new StringWriter(4096);
             String line = null;
             while ( (line = in.readLine()) != null ) {
               if ( (state & 1) == 0 ) {
                 // try to copy the XML line in any case
                 if ( (p1 = line.indexOf( "<?xml" )) > -1 )
                   if ( (p2 = line.indexOf( "?>", p1 )) > -1 ) {
 //                    out.write( line, p1, p2+2 );
                     System.out.println( "state=" + state + ", seen <?xml ...?>" );
                   }
                 // start state with the correct root element
                 if ( (p1 = line.indexOf( "<invocation")) > -1 ) {
                   if ( p1 > 0 ) line = line.substring( p1 );
                   System.out.println( "state=" + state + ", seen <invocation>" );
                   out.write(XMLHEADER);
                   ++state;
                 }
               }
               if ( (state & 1) == 1 ) {
                 out.write( line );
                 if ( (p1 = line.indexOf("</invocation>")) > -1 ) {
                   System.out.println( "state=" + state + ", seen </invocation>" );
                   ++state;

                   out.flush();
                   out.close();
                   result.add( out.toString() );
                   out = new StringWriter(4096);
                 }
               }
             }

             in.close();
             out.close();
           } catch ( IOException ioe ) {
             throw new Exception( "While copying " + input.getPath() +
                                      " into temp. file: " + ioe.getMessage() );
         }


      // some sanity checks
      if ( state == 0 )
        throw new Exception( "File " + input.getPath() +
                                 " does not contain invocation records," +
                                 " assuming failure");
      if ( (state & 1) == 1 )
        throw new Exception( "File " + input.getPath() +
                                 " contains an incomplete invocation record," +
                                 " assuming failure" );

      // done
      return result;
  }

private void parseFiles(List jobs)throws Exception{
    File infile = null;
    File outfile = null;
    List ilist = null;
    List temp = new ArrayList(jobs);
    for (Iterator i = temp.iterator(); i.hasNext(); ) {
        String job = (String) i.next();
        if (job.startsWith("rc_tx_")) {
            //this is for stagein jobs
            outfile = new File(job + ".out.lof");
            if (outfile.exists() && outfile.canRead() && outfile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(outfile));
                    String str;
                    while ( (str = in.readLine()) != null) {
                        if (output == null) {
                            output = new HashMap();
                        }
                        if (!output.containsKey(job)) {
                            output.put(job, new ArrayList());
                        }
                        ilist = (List) output.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }

        }else if (job.startsWith("new_rc_tx_")) {
            //this is for stageout/inter tx jobs
            outfile = new File(job + ".out.lof");
            if (outfile.exists() && outfile.canRead() && outfile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(outfile));
                    String str;
                    while ( (str = in.readLine()) != null) {
                        if (input == null) {
                            input = new HashMap();
                        }
                        if (!input.containsKey(job)) {
                            input.put(job, new ArrayList());
                        }
                        ilist = (List) input.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }

        }else if(job.startsWith("inter_tx_")){
            outfile = new File(job + ".out.lof");
            if (outfile.exists() && outfile.canRead() && outfile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(outfile));
                    String str;
                    while ( (str = in.readLine()) != null) {
                        if (output == null) {
                            output = new HashMap();
                        }
                        if (!output.containsKey(job)) {
                            output.put(job, new ArrayList());
                        }
                        ilist = (List) output.get(job);
                        ilist.add(str);
                        if (input == null) {
                            input = new HashMap();
                        }
                        if (!input.containsKey(job)) {
                            input.put(job, new ArrayList());
                        }
                        ilist = (List) input.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }

        } else if(job.startsWith("new_rc_register")){
            BufferedReader bf =new BufferedReader(new FileReader(new File(job+".in")));
            String line = null;
            while((line=bf.readLine())!=null){
                String lfn=null;
                lfn= line.split(" ")[0];
                if(input==null){
                    input=new HashMap();
                }
                if(!input.containsKey(job)){
                    input.put(job, new ArrayList());
                }
                ilist=(List)input.get(job);
                ilist.add(lfn);
            }
            bf.close();
        }else if (job.startsWith("cln_")) {
            //this is for cleanup jobs
            infile = new File(job + ".in.lof");
            if (infile.exists() && infile.canRead() && infile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(infile));
                    String str;
                    while ( (str = in.readLine()) != null) {

                        if (input == null) {
                            input = new HashMap();
                        }
                        if (!input.containsKey(job)) {
                            input.put(job, new ArrayList());
                        }
                        ilist = (List) input.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }
        } else if (!job.endsWith("_cdir")) {
            //this is a regular job
            outfile = new File(job + ".out.lof");
            if (outfile.exists() && outfile.canRead() && outfile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(outfile));
                    String str;
                    while ( (str = in.readLine()) != null) {
                        if (output == null) {
                            output = new HashMap();
                        }
                        if (!output.containsKey(job)) {
                            output.put(job, new ArrayList());
                        }
                        ilist = (List) output.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }

            infile = new File(job + ".in.lof");
            if (infile.exists() && infile.canRead() && infile.length() != 0) {
                try {
                    BufferedReader in = new BufferedReader(new FileReader(infile));
                    String str;
                    while ( (str = in.readLine()) != null) {

                        if (input == null) {
                            input = new HashMap();
                        }
                        if (!input.containsKey(job)) {
                            input.put(job, new ArrayList());
                        }
                        ilist = (List) input.get(job);
                        ilist.add(str);
                    }
                    in.close();
                }
                catch (IOException e) {
                }
            }
        }
    }
}

    private void parseDaxFile(String file, List jobs)throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder       db = dbf.newDocumentBuilder();
        Document      msgDoc = db.parse(new File(file));
        NodeList nlist = msgDoc.getElementsByTagName("job");
        List temp = new ArrayList(jobs);
        input = new HashMap();
        output = new HashMap();

        for (int i =0;i<nlist.getLength();i++){
            String tempname=nlist.item(i).getAttributes().getNamedItem("name").getNodeValue()+"_"+nlist.item(i).getAttributes().getNamedItem("id").getNodeValue();
            if(temp.contains(tempname)){
                temp.remove(tempname);
                NodeList uselist = nlist.item(i).getChildNodes();
                for (int j=0;j<uselist.getLength();j++){
                    if(uselist.item(j).getNodeName().equals("uses")){
                        Node n = uselist.item(j).getAttributes().getNamedItem("link");
                        if(n!=null) {
                            List ilist = null;
                            String fname = uselist.item(j).getAttributes().getNamedItem("file").getNodeValue();
                            if(n.getNodeValue().equalsIgnoreCase("output")){

                                if(output==null){
                                    output = new HashMap();
                                    ilist = new ArrayList();
                                    output.put(tempname,ilist);
                                }
                                if(!output.containsKey(tempname)){
                                    output.put(tempname,new ArrayList());
                                }
                                ilist=(List)output.get(tempname);
                                ilist.add(fname);
                            } else {
                                if(input==null){
                                    input=new HashMap();
                                }
                                if(!input.containsKey(tempname)){
                                    input.put(tempname,new ArrayList());
                                }
                                ilist=(List)input.get(tempname);
                                ilist.add(fname);
                            }
                        }

                    }
                }
            }
        }
    }

    private void parseInput() throws Exception{
        if(parents!=null && !parents.isEmpty()){
            for(Iterator p=parents.iterator();p.hasNext();){
                String tempjob=(String)p.next();
                if(tempjob.startsWith("rc_tx") || tempjob.startsWith("inter_tx") ){
                    List ilist=null;
                    if(output==null){
                        output = new HashMap();
                    }
                    if(!output.containsKey(tempjob)){
                        output.put(tempjob,new ArrayList());
                    }
                    ilist=(List)output.get(tempjob);
                    BufferedReader bf =new BufferedReader(new FileReader(new File(tempjob+".in")));
                    String line = null;
                    while((bf.readLine())!=null){
                        bf.readLine();
                        bf.readLine();
                        line=bf.readLine();
                        filecount++;
                        String lfn= line.split("run\\d{4}/")[1];
                        ilist.add(lfn);
                    }
                    bf.close();
                }

            }
        }
        if(children!=null && !children.isEmpty()){
            for(Iterator c=children.iterator();c.hasNext();){
                String tempjob=(String)c.next();
                if(tempjob.startsWith("new_rc_tx") || tempjob.startsWith("inter_tx") ){
                    List ilist=null;
                    if(input==null){
                        input = new HashMap();

                    }
                    if(!input.containsKey(tempjob)){
                        input.put(tempjob,new ArrayList());
                    }
                    ilist=(List)input.get(tempjob);
                    BufferedReader bf =new BufferedReader(new FileReader(new File(tempjob+".in")));
                    String line = null;
                    while((bf.readLine())!=null){

                        line=bf.readLine();
                        filecount++;
                        String lfn= line.split("run\\d{4}/")[1];
                        ilist.add(lfn);
                        bf.readLine();
                        bf.readLine();
                    }
                    bf.close();

                }

            }
        }
        if(jobname.startsWith("rc_tx")||jobname.startsWith("new_rc_tx")||jobname.startsWith("inter_tx")){
            BufferedReader bf =new BufferedReader(new FileReader(new File(jobname+".in")));
            String line = null;
            List ilist=null;
            while((bf.readLine())!=null){
                String lfn=null;
                line=bf.readLine();
                if(jobname.startsWith("new_rc_tx") || jobname.startsWith("inter_tx")){
                    lfn= line.split("run\\d{4}/")[1];
                }
                if(input==null){
                    input=new HashMap();
                }
                input.put(jobname,new ArrayList());
                if(input.containsKey(jobname)){
                    ilist=(List)input.get(jobname);
                }
                ilist.add(lfn);
                bf.readLine();
                line=bf.readLine();
                if(jobname.startsWith("rc_tx")||jobname.startsWith("inter_tx")){
                    lfn= line.split("run\\d{4}/")[1];

                }
                if(output==null){
                    output=new HashMap();
                }
                output.put(jobname,new ArrayList());
                if(output.containsKey(jobname)){
                    ilist=(List)output.get(jobname);
                }
                ilist.add(lfn);

            }
            bf.close();
        }

        if(jobname.startsWith("new_rc_register")){
            BufferedReader bf =new BufferedReader(new FileReader(new File(jobname+".in")));
            String line = null;
            List ilist=null;
            while((line=bf.readLine())!=null){
                String lfn=null;
                lfn= line.split(" ")[0];
                if(input==null){
                    input=new HashMap();
                }
                input.put(jobname,new ArrayList());
                if(input.containsKey(jobname)){
                    ilist=(List)input.get(jobname);
                }
                ilist.add(lfn);
            }
            bf.close();
        }
    }

    public InteractionKey jobInvocationInteraction() throws Exception{

        System.out.println("We now create the job Invocation interaction key");

        // Create addresses for the source and sink of the
        // interaction.
        WSAddressEndpoint source = new WSAddressEndpoint(CONDOR);

        WSAddressEndpoint sink   = new WSAddressEndpoint(jobname);


        // Create an interactionId, this should be unique!

        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);


        System.out.println("Building p-assertions...");

        InteractionPAssertion ipa = createJobInvocationInteractionPAssertion();


        //setting sender type
        System.out.println("We are the sender/client view of the interaction");
        String vk = Constants.SENDER_VIEW_TYPE;
        System.out.println();

        //set asserter to CONDOR

        WSAddressEndpoint asserter = new WSAddressEndpoint(CONDOR);

        List records = new ArrayList();

        System.out.println("Creating Record objects for each p-assertion");

        Record recIpa = new Record(ipa, ik, vk, asserter.getElement());
        records.add(recIpa);

        //iterate over parents to create multiple rpa's
        RelationshipPAssertion rpa = null;
        Record recRpa=null;
        if(input.containsKey(jobname)){
            List inputs = (List)input.get(jobname);
            //    for(int i=0; i<inputs.size();i++){
            //       Iterator j = inputs.iterator();
            int i=0;
            for(Iterator j = inputs.iterator();j.hasNext();){
                String tempfile=(String)j.next();
                for(Iterator k = parents.iterator();k.hasNext();){
                    String tempjob=(String)k.next();
                    List templist=(List)output.get(tempjob);
                    if(templist!=null){
                        if (templist.contains(tempfile)){
                            i++;
                            System.out.println("Parent Relationship *** file="+tempfile+" from="+jobname+" to="+tempjob);
                            recRpa=new Record(createJobToTransferRelationshipPAssertion(tempfile,tempjob,i ),ik,vk,asserter.getElement());
                            records.add(recRpa);
                        }
                    }
                }

            }
        }

        System.out.println("Recording the p-assertions in provenance store " + provenanceStore);

        clientLib.record(records.iterator(), provenanceStore,true);


        System.out.println("sender p-assertions recorded");
        System.out.println();

        //setting reciever type

        System.out.println("We are the sender/client view of the interaction");
        vk = Constants.RECEIVER_VIEW_TYPE;
        System.out.println();

        //set asserter to Job

        asserter = new WSAddressEndpoint(jobname);
        recIpa = new Record(ipa, ik, vk, asserter.getElement());



        System.out.println("Recording the p-assertions in provenance store " + provenanceStore);

        clientLib.record(recIpa, provenanceStore);


        System.out.println("receiver p-assertions recorded");
        System.out.println();

        return ik;

    }

    public void jobCompletionInteraction(InteractionKey invocationinteractionkey) throws Exception{

        System.out.print("Creating Completion Interaction Key ....... ");

        // Create addresses for the source and sink of the
        // interaction.
        // Create an interactionId, this should be unique!
        WSAddressEndpoint source = new WSAddressEndpoint(jobname);
        WSAddressEndpoint sink   = new WSAddressEndpoint(CONDOR);
        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);
        System.out.println("DONE");

        //setting sender type
        String vk = Constants.SENDER_VIEW_TYPE;
        //set asserter to be the job
        WSAddressEndpoint asserter = new WSAddressEndpoint(jobname);

        System.out.println("Building p-assertions ..... ");
        List records = new ArrayList();

        InteractionPAssertion ipa = createJobCompletionInteractionPAssertion();
        Record recIpa = new Record(ipa, ik, vk, asserter.getElement());
        records.add(recIpa);
        //iterate over files to create multiple rpa's
        RelationshipPAssertion rpa = null;
        Record recRpa=null;
        if(output.containsKey(jobname)){
            int count=0;
            for(Iterator i=((List)output.get(jobname)).iterator(); i.hasNext();){
                count++;
                recRpa=new Record(createJobRelationshipPAssertion(invocationinteractionkey,(String)i.next(),count ) ,ik,vk,asserter.getElement());
                records.add(recRpa);
            }
        }
        ActorStatePAssertion apa = createActorStatePAssertion(0);

        Record recApa = new Record(apa, ik, vk, asserter.getElement());

        records.add(recApa);
        System.out.print("Recording the sender p-assertions in provenance store ..... ");

        clientLib.record(records.iterator(), provenanceStore,true);

        System.out.println("DONE");

        //setting reciever type
        vk = Constants.RECEIVER_VIEW_TYPE;


        //set asserter to CONDOR

        asserter = new WSAddressEndpoint(CONDOR);
        recIpa = new Record(ipa, ik, vk, asserter.getElement());

        records = new ArrayList();
        records.add(recIpa);
        /**
         * //iterate over children to create multiple rpa's
         * rpa = null;
         * recRpa=null;
         * List outputs = (List)output.get(jobname);
         * //     for(int i=0; i<outputs.size();i++){
         * //         Iterator j = outputs.iterator();
         * int i =0;
         * for(Iterator j=outputs.iterator();j.hasNext();){
         *
         * String tempfile=(String)j.next();
         * for(Iterator k = children.iterator();k.hasNext();){
         * String tempjob=(String)k.next();
         * List templist = (List)input.get(tempjob);
         * if(templist!=null){
         * if(templist.contains(tempfile)){
         * i++;
         * System.out.println("Child Relationship *** file="+tempfile+" from="+jobname+" to="+tempjob);
         *
         * recRpa=new Record(createJobToTransferRelationshipPAssertion(tempfile,tempjob,i ),ik,vk,asserter.getElement());
         * records.add(recRpa);
         * }
         * }
         * }
         *
         * }
         **/
        System.out.print("Recording the receiver p-assertions in provenance store ..... ");

        clientLib.record(recIpa, provenanceStore);


        System.out.println("Done");

    }

    public InteractionKey transferInvocationInteraction() throws Exception{

        System.out.print("Creating Invocation Interaction Key ..... ");

        // Create addresses for the source and sink of the
        // interaction.
        WSAddressEndpoint source = new WSAddressEndpoint(CONDOR);
        WSAddressEndpoint sink   = new WSAddressEndpoint(jobname);
        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);
        System.out.println("Done");

        //setting sender type
        String vk = Constants.SENDER_VIEW_TYPE;
        //set asserter to CONDOR
        WSAddressEndpoint asserter = new WSAddressEndpoint(CONDOR);


        System.out.print("Building p-assertions ..... ");
        InteractionPAssertion ipa = createTransferInvocationInteractionPAssertion();

        List records=new ArrayList();
        Record recIpa = new Record(ipa, ik, vk, asserter.getElement());
        records.add(recIpa);
        if(!jobname.startsWith("rc_tx")){
            //iterate over parents to create multiple rpa's
            RelationshipPAssertion rpa = null;
            Record recRpa=null;
            List inputs = (List)input.get(jobname);
            //    for(int i=0; i<inputs.size();i++){
            //       Iterator j = inputs.iterator();
            int i=0;
            for(Iterator j = inputs.iterator();j.hasNext();){
                String tempfile=(String)j.next();
                for(Iterator k = parents.iterator();k.hasNext();){
                    String tempjob=(String)k.next();
                    List templist=(List)output.get(tempjob);
                    if(templist!=null){
                        if (templist.contains(tempfile)){
                            i++;
                            //  System.out.println("Parent Relationship *** file="+tempfile+" from="+jobname+" to="+tempjob);
                            recRpa=new Record(createJobToTransferRelationshipPAssertion(tempfile,tempjob,i ),ik,vk,asserter.getElement());
                            records.add(recRpa);
                        }
                    }
                }

            }
        }
        System.out.println("Done");
        System.out.print("Recording the sender p-assertions in provenance store .......... ");
        clientLib.record(records.iterator(), provenanceStore,true);
        System.out.println("Done");

        //setting reciever type
        vk = Constants.RECEIVER_VIEW_TYPE;
        //set asserter to job type
        asserter = new WSAddressEndpoint(jobname);
        //add the interaction P assertion
        recIpa = new Record(ipa, ik, vk, asserter.getElement());
        System.out.print("Recording the receiver p-assertions in provenance store ........ ");
        clientLib.record(recIpa, provenanceStore);
        System.out.println("DONE");
        return ik;

    }

    public void transferCompletionInteraction(InteractionKey invocationinteractionkey) throws Exception{

        System.out.print("Creating Completion Interaction Key ....... ");
        // Create addresses for the source and sink of the
        // interaction.
        WSAddressEndpoint source = new WSAddressEndpoint(jobname);
        WSAddressEndpoint sink   = new WSAddressEndpoint(CONDOR);
        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);
        System.out.println("Done");

        //setting sender type
        String vk = Constants.SENDER_VIEW_TYPE;
        //set asserter to the job type
        WSAddressEndpoint asserter = new WSAddressEndpoint(jobname);

        System.out.print("Building p-assertions ..... ");
        List records = new ArrayList();

        InteractionPAssertion ipa = createTransferCompletionInteractionPAssertion();
        Record recIpa = new Record(ipa, ik, vk, asserter.getElement());
        records.add(recIpa);

        //iterate over files to create multiple rpa's
        RelationshipPAssertion rpa = null;
        Record recRpa=null;

        //get this file number from the .in file
        //simon or paul will change the client.record method to take iterator of assertions instead of iterator of records.
        for(int i=0; i<filecount;i++){
            recRpa=new Record(createTransferRelationshipPAssertion(invocationinteractionkey,i ) ,ik,vk,asserter.getElement());
            records.add(recRpa);
        }

        ActorStatePAssertion apa = createActorStatePAssertion(0);
        Record recApa = new Record(apa, ik, vk, asserter.getElement());
        records.add(recApa);

        System.out.println("DONE");

        System.out.print("Recording the sender p-assertions in provenance store ..... ");
        clientLib.record(records.iterator(), provenanceStore,true);
        System.out.println("Done");

        //setting reciever type

        vk = Constants.RECEIVER_VIEW_TYPE;
        //set asserter to CONDOR
        asserter = new WSAddressEndpoint(CONDOR);

        //adding the interaction p assertion
        recIpa = new Record(ipa, ik, vk, asserter.getElement());
        System.out.print("Recording the receiver p-assertions in provenance store .... ");

        clientLib.record(recIpa, provenanceStore);
        System.out.println("DONE");

    }

    private InteractionPAssertion createTransferInvocationInteractionPAssertion()
    throws Exception {
        // Create an interaction p-assertion
        // First we make a local p-assertion id and then
        // we make a documentationStyle. In this case we
        // call it verbatium.
        //
        // In most cases, you'll be grabing the messageBody from the message
        // being sent between parties. So a SOAP message, or a CORBA message.
        // With this example we'll just use a hard coded message body.

        String localPAssertionId = "1";

        // this message content will be obtained by parsing the transfer input files <jobid.in> and obtaining the source urls

        BufferedReader bf =new BufferedReader(new FileReader(new File(jobname+".in")));
        String line = null;
        StringBuffer  message = new StringBuffer("<transfer xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/transfer\">");
        while((bf.readLine())!=null){
            line=bf.readLine();
            filecount++;
            if(!jobname.startsWith("new_rc_tx")){
                message.append("<filename>"+line+"</filename>");
            } else {
                String lfn= line.split("run\\d{4}/")[1];

                message.append("<filename file=\""+lfn+"\">"+line+"</filename>");
            }
            bf.readLine();
            bf.readLine();
        }
        bf.close();
        message.append("</transfer>");

        // Convert it into a DOM Element
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
        Element messageBody = msgDoc.getDocumentElement();

        InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

        return ipa;
    }

    private InteractionPAssertion createTransferCompletionInteractionPAssertion()
    throws Exception {
        // Create an interaction p-assertion
        // First we make a local p-assertion id and then
        // we make a documentationStyle. In this case we
        // call it verbatium.
        //
        // In most cases, you'll be grabing the messageBody from the message
        // being sent between parties. So a SOAP message, or a CORBA message.
        // With this example we'll just use a hard coded message body.

        String localPAssertionId = "1";

        // this message content will be obtained by parsing the transfer input files <jobid.in> and obtaining the destination urls


        BufferedReader bf =new BufferedReader(new FileReader(new File(jobname+".in")));
        String line = null;
        StringBuffer  message = new StringBuffer("<transfer xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/transfer\">");
        while((line=bf.readLine())!=null){
            bf.readLine();
            bf.readLine();
            line = bf.readLine();
            filecount++;
            if(jobname.startsWith("new_rc_tx")){
                message.append("<filename>"+line+"</filename>");
            }else {
                String lfn= line.split("run\\d{4}/")[1];
                message.append("<filename file=\""+lfn+"\">"+line+"</filename>");
            }
        }
        bf.close();
        message.append("</transfer>");

        // Convert it into a DOM Element
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
        Element messageBody = msgDoc.getDocumentElement();

        InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

        return ipa;
    }

    private ActorStatePAssertion createActorStatePAssertion(long count)
    throws Exception {
        // Create an actor state p-assertion
        // Just like the interaction p-assertion this p-assertion
        // needs a local p-assertion id. Remember, all the p-assertions
        // in one view need a different id. Therefore, we give this assertion
        // the id of "2" instead of "1".
        //
        // Again you'll typically be getting some state from the actor,
        // translating it to XML to create the actor state p-assertion
        // In this example, we just use a hard coded string.

        String localPAssertionId = "aspa-"+count;

        ActorStatePAssertion asa = new ActorStatePAssertion(localPAssertionId, docelement);

        return asa;
    }

    private RelationshipPAssertion createTransferRelationshipPAssertion(InteractionKey invocationik, long index)
    throws Exception {
        // Create a relationship p-assertion
        // Again a different local p-assertion id
        //
        // We'll create a "usage" relationship between the interaction p-assertion
        // and the actor state p-assertion. This relationship says that
        // message represented by interaction p-assertion "1" used the actor state
        // represented by actor state p-assertion "2".
        // There are no data accessors or links so we pass null.

        // Create the information to identify the subject of the relationship
        // Remember, parameter names must be identified and they need to be URIs
        String localPAssertionId = "rpa"+index;
        String subjectLocalPAssertionId = "1";
        String subjectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/outputfile";

        // Create the information to identify the object of the relationship

        String objectLocalPAssertionId = "1"; // points to the interaction p-assertion of the invocation interaction receiver


        GlobalPAssertionKey gpak = new GlobalPAssertionKey(invocationik, "receiver", objectLocalPAssertionId);
        String objectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/inputfile";

        Element dataAccessor= createTransferDataAccessor(index);

        ObjectID objId = new ObjectID(gpak, objectParameterName, dataAccessor, null);

        // We add the objId to the list of objects. We only have one objectId here
        // but when making another type of relationship more than one objectId may
        // be required
        LinkedList objectIds = new LinkedList();
        objectIds.add(objId);

        // Create the "use" relation. Again this should be a URI
        String relation = "http://pegasus.isi.edu/pasoa/relation/transfer/copy-of";
        dataAccessor= createTransferDataAccessor( index);
        // Finally, create the relationship object and return it.
        RelationshipPAssertion rel = new RelationshipPAssertion(localPAssertionId,
                subjectLocalPAssertionId,
                dataAccessor,
                subjectParameterName,
                relation,
                objectIds);

        return rel;

    }

    //will have to do for handling merged jobs correctly.
    private InteractionPAssertion createMergedJobInvocationInteractionPAssertion() throws Exception{

      String localPAssertionId = "1";

      // this message content will be obtained by parsing the transfer input files <jobid.in> and obtaining the source urls
      StringBuffer  message = new StringBuffer("<files link=\"input\" xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/files\">");
      if(input!=null){
           if(input.containsKey(jobname)){
               List inputs = (List) input.get(jobname);
               for (Iterator i = inputs.iterator(); i.hasNext(); ) {
                   message.append("<filename>" + (String) i.next() +
                                  "</filename>");
               }
           }
      }
      message.append("</files>");

      // Convert it into a DOM Element
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
      Element messageBody = msgDoc.getDocumentElement();

      InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

      return ipa;

  }


    private InteractionPAssertion createJobInvocationInteractionPAssertion() throws Exception{

        String localPAssertionId = "1";

        // this message content will be obtained by parsing the transfer input files <jobid.in> and obtaining the source urls

        StringBuffer  message = new StringBuffer("<files link=\"input\" xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/files\">");
        if(input!=null){
             if(input.containsKey(jobname)){
                 List inputs = (List) input.get(jobname);
                 for (Iterator i = inputs.iterator(); i.hasNext(); ) {
                     message.append("<filename>" + (String) i.next() +
                                    "</filename>");
                 }
             }
        }
        message.append("</files>");

        // Convert it into a DOM Element
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
        Element messageBody = msgDoc.getDocumentElement();

        InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

        return ipa;

    }



    private InteractionPAssertion createJobCompletionInteractionPAssertion() throws Exception{

        String localPAssertionId = "1";

        // this message content will be obtained by parsing the transfer input files <jobid.in> and obtaining the source urls

        StringBuffer  message = new StringBuffer("<files link=\"output\" xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/files\">");
        if(output!=null){
            if(output.containsKey(jobname)){
                List outputs = (List) output.get(jobname);
                for (Iterator i = outputs.iterator(); i.hasNext(); ) {
                    message.append("<filename>" + (String) i.next() +
                                   "</filename>");
                }
            }
        }
        message.append("</files>");

        // Convert it into a DOM Element
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
        Element messageBody = msgDoc.getDocumentElement();

        InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

        return ipa;
    }

    private RelationshipPAssertion createJobRelationshipPAssertion(InteractionKey invocationik, String filename, long index)
    throws Exception {
        String localPAssertionId = "rpa"+index;
        String subjectLocalPAssertionId = "1";
        String subjectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/outputfile";

        // Create the information to identify the object of the relationship

        String objectLocalPAssertionId = "1"; // points to the interaction p-assertion of the invocation interaction receiver


        GlobalPAssertionKey gpak = new GlobalPAssertionKey(invocationik, "receiver", objectLocalPAssertionId);
        String objectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/inputfile";
        LinkedList objectIds = new LinkedList();

        for(Iterator i=((List)input.get(jobname)).iterator();i.hasNext();){
            Element dataAccessor= createLFNDataAccessor((String)i.next());
            // We add the objId to the list of objects. We only have one objectId here
            // but when making another type of relationship more than one objectId may
            // be required
            objectIds.add(new ObjectID(gpak, objectParameterName, dataAccessor, null));
        }

        // Create the "use" relation. Again this should be a URI
        String relation = "http://pegasus.isi.edu/pasoa/relation/transformation/product-of";
        Element dataAccessor= createLFNDataAccessor(filename);

        // Finally, create the relationship object and return it.
        RelationshipPAssertion rel = new RelationshipPAssertion(localPAssertionId,
                subjectLocalPAssertionId,
                dataAccessor,
                subjectParameterName,
                relation,
                objectIds);

        return rel;

    }
    private RelationshipPAssertion createJobToTransferRelationshipPAssertion(String filename,String parentjob,int index) throws Exception{
        String localPAssertionId = "rpa"+index;
        String subjectLocalPAssertionId = "1";
        String subjectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/inputfile";

        // Create the information to identify the object of the relationship

        String objectLocalPAssertionId = "1"; // points to the interaction p-assertion of the invocation interaction receiver
        // interaction.

        WSAddressEndpoint source = new WSAddressEndpoint(parentjob);
        WSAddressEndpoint sink   = new WSAddressEndpoint(CONDOR);

        String interactionId =  wf_label+wf_planned_time+parentjob;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);

        GlobalPAssertionKey gpak = new GlobalPAssertionKey(ik, "receiver", objectLocalPAssertionId);
        String objectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/outputfile";

        Element dataAccessor= createLFNDataAccessor(filename);

        ObjectID objId = new ObjectID(gpak, objectParameterName, dataAccessor, null);

        // We add the objId to the list of objects. We only have one objectId here
        // but when making another type of relationship more than one objectId may
        // be required
        LinkedList objectIds = new LinkedList();
        objectIds.add(objId);

        // Create the "use" relation. Again this should be a URI
        String relation = "http://pegasus.isi.edu/pasoa/relation/transfer/same-as";
        //     dataAccessor=createNameValueDataAccessor(filename);
        // Finally, create the relationship object and return it.
        RelationshipPAssertion rel = new RelationshipPAssertion(localPAssertionId,
                subjectLocalPAssertionId,
                dataAccessor,
                subjectParameterName,
                relation,
                objectIds);

        return rel;
    }
    private Element createTransferDataAccessor(long index){
        Map namespaces = new HashMap();
        namespaces.put("tr", "http://pegasus.isi.edu/schema/pasoa/content/transfer");
        return new org.pasoa.accessors.snxpath.SingleNodeXPathManager().createAccessor("/tr:transfer[0]/tr:filename[" + index + "]",
                namespaces);

    }

    private Element createLFNDataAccessor(String value){
        return new org.pasoa.accessors.lfn.LFNAccessorManager().createLFNAccessor(value);
    }

    private InteractionPAssertion createRegisterInvocationInteractionPAssertion()
    throws Exception {
        // Create an interaction p-assertion
        // First we make a local p-assertion id and then
        // we make a documentationStyle. In this case we
        // call it verbatium.
        //
        // In most cases, you'll be grabing the messageBody from the message
        // being sent between parties. So a SOAP message, or a CORBA message.
        // With this example we'll just use a hard coded message body.

        String localPAssertionId = "1";


        BufferedReader bf =new BufferedReader(new FileReader(new File(jobname+".in")));
        String line = null;
        StringBuffer  message = new StringBuffer("<register xmlns=\"http://pegasus.isi.edu/schema/pasoa/content/register\">");
        while((line=bf.readLine())!=null){
            filecount++;
            String[] lfn= line.split(" ");
            message.append("<filename file=\""+lfn[0]+"\">"+lfn[1]+"</filename>");

        }
        message.append("</register>");

        // Convert it into a DOM Element
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document msgDoc = db.parse(new InputSource(new StringReader(message.toString())));
        Element messageBody = msgDoc.getDocumentElement();

        InteractionPAssertion ipa = new InteractionPAssertion(localPAssertionId, documentationStyle, messageBody);

        return ipa;
    }

    public InteractionKey registerInvocationInteraction() throws Exception{

        System.out.println("We now create the transfer Invocation interaction key");

        // Create addresses for the source and sink of the
        // interaction.
        WSAddressEndpoint source = new WSAddressEndpoint(CONDOR);
        WSAddressEndpoint sink   = new WSAddressEndpoint(jobname);


        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);

        System.out.println("Building p-assertions...");

        InteractionPAssertion ipa = createRegisterInvocationInteractionPAssertion();

        List records=new ArrayList();
        //setting sender type
        System.out.println("We are the sender/client view of the interaction");
        String vk = Constants.SENDER_VIEW_TYPE;
        System.out.println();

        //set asserter to CONDOR

        WSAddressEndpoint asserter = new WSAddressEndpoint(CONDOR);


        System.out.println("Creating Record objects for each p-assertion");

        Record recIpa = new Record(ipa, ik, vk, asserter.getElement());
        records.add(recIpa);
        Record recRpa = null;

        String tempparent=null;
        if(parents !=null || !parents.isEmpty()){
            tempparent=(String)parents.get(0);
        }
        for(int i=0; i<filecount;i++){
            recRpa=new Record(createRegisterToTransferRelationshipPAssertion(tempparent,i ) ,ik,vk,asserter.getElement());
            records.add(recRpa);
        }
        System.out.println("Recording the p-assertions in provenance store " + provenanceStore);

        clientLib.record(records.iterator(), provenanceStore,true);


        System.out.println("sender p-assertions recorded");
        System.out.println();

        //setting reciever type

        System.out.println("We are the sender/client view of the interaction");
        vk = Constants.RECEIVER_VIEW_TYPE;
        System.out.println();


        //set asserter to CONDOR

        asserter = new WSAddressEndpoint(jobname);
        recIpa = new Record(ipa, ik, vk, asserter.getElement());


        System.out.println("Recording the p-assertions in provenance store " + provenanceStore);

        clientLib.record(recIpa, provenanceStore);


        System.out.println("receiver p-assertions recorded");
        System.out.println();

        return ik;

    }

    public void registerCompletionInteraction(InteractionKey invocationinteractionkey) throws Exception{

        System.out.println("We now create the register Completion interaction key");

        // Create addresses for the source and sink of the
        // interaction.
        WSAddressEndpoint source = new WSAddressEndpoint(jobname);
        WSAddressEndpoint sink   = new WSAddressEndpoint(CONDOR);


        String interactionId =  wf_label+wf_planned_time+jobname;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);

        System.out.println("Building p-assertions...");
        List records = new ArrayList();
        //setting sender type
        System.out.println("We are the sender/client view of the interaction");
        String vk = Constants.SENDER_VIEW_TYPE;
        System.out.println();


        WSAddressEndpoint asserter = new WSAddressEndpoint(jobname);

        System.out.println("Creating Record objects for each p-assertion ....... ");

        ActorStatePAssertion apa = createActorStatePAssertion(0);
        System.out.println("Done");
        Record recApa = new Record(apa, ik, vk, asserter.getElement());

        records.add(recApa);
        System.out.print("Recording sender p-assertions ............ ");

        clientLib.record(records.iterator(), provenanceStore,true);

        System.out.println("DONE\n");

        //setting reciever type

        System.out.println("We are the sender/client view of the interaction\n");
        vk = Constants.RECEIVER_VIEW_TYPE;
        asserter = new WSAddressEndpoint(CONDOR);

        //no receiver InteractionPAssertion.


    }

    private RelationshipPAssertion createRegisterToTransferRelationshipPAssertion(String parentjob,  long index)
    throws Exception {
        // Create a relationship p-assertion
        // Again a different local p-assertion id
        //
        // We'll create a "usage" relationship between the interaction p-assertion
        // and the actor state p-assertion. This relationship says that
        // message represented by interaction p-assertion "1" used the actor state
        // represented by actor state p-assertion "2".
        // There are no data accessors or links so we pass null.

        // Create the information to identify the subject of the relationship
        // Remember, parameter names must be identified and they need to be URIs
        String localPAssertionId = "rpa"+index;
        String subjectLocalPAssertionId = "1";
        String subjectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/outputfile";

        // Create the information to identify the object of the relationship

        String objectLocalPAssertionId = "1"; // points to the interaction p-assertion of the invocation interaction receiver


        WSAddressEndpoint source = new WSAddressEndpoint(parentjob);
        WSAddressEndpoint sink   = new WSAddressEndpoint(CONDOR);

        String interactionId =  wf_label+wf_planned_time+parentjob;
        InteractionKey ik = new InteractionKey(source.getElement(), sink.getElement(), interactionId);

        GlobalPAssertionKey gpak = new GlobalPAssertionKey(ik, "receiver", objectLocalPAssertionId);
        String objectParameterName = "http://pegasus.isi.edu/schema/pasoa/type/inputfile";

        Element dataAccessor= createTransferDataAccessor(index);

        ObjectID objId = new ObjectID(gpak, objectParameterName, dataAccessor, null);

        // We add the objId to the list of objects. We only have one objectId here
        // but when making another type of relationship more than one objectId may
        // be required
        LinkedList objectIds = new LinkedList();
        objectIds.add(objId);

        // Create the "use" relation. Again this should be a URI
        String relation = "http://pegasus.isi.edu/pasoa/relation/register/rls-mapping";
        // Finally, create the relationship object and return it.
        RelationshipPAssertion rel = new RelationshipPAssertion(localPAssertionId,
                subjectLocalPAssertionId,
                dataAccessor,
                subjectParameterName,
                relation,
                objectIds);

        return rel;

    }
}
