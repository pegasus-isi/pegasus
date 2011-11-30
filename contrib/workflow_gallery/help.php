<?php  
	include_once( $_SERVER['DOCUMENT_ROOT']."/static/includes/common.inc.php" );
	do_html_header("Documentation");
?>
<div id = "header_div" class ="header">
	<h1>Workflow gallery</h1>
</div>
<div id="main" class ="columns">
	<div id="left_div" class ="left">
	<a href ="index.php">Home</a>
	</div>
	<div id="right_div" class ="right" >
	</div>
	<div id="center_div" class ="middle">
		<center>
		<p>The workflow gallery is a gallery of archived workflow runs on distributed resources. The workflow gallery is divided into three main pages.
The <a href = 'index.php'>main page </a> lists various types of workflows. It provides a description of each workflow type. Selecting a particular 
workflow type will takes you to the workflow type page. It shows all the runs of that workflow type along with a short summary of the workflow .
<br/>The summary contains the following details. For workflows with sub workflows the  summary is across all sub workflows.i.e. For workflows
having sub workflow jobs (i.e SUBDAG and SUBDAX jobs), the value includes jobs from the sub workflows as well. <br/>
		<table style="color: rgb(96, 0, 0);">
		<tbody><tr><th style="color: rgb(96, 0, 0);"><pre>Workflow runtime(min,sec)         :</pre></th><td style="color: rgb(136, 136, 136);">the walltime from the start of the workflow execution to the end as reported by the DAGMAN.In case of rescue dag the value is the cumulative of all retries.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre>Cumulative workflow runtime(min,sec):</pre></th><td style="color: rgb(136, 136, 136);">the sum of the walltime of all jobs as reported by the DAGMan .In case of job retries the value is the cumulative of all retries.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre>Total jobs run                     :</pre></th><td style="color: rgb(136, 136, 136);">the total number of jobs runs during the workflow run . In case of a failed workflow the number of jobs run could be less than the total jobs in the planned workflow. This is a runtime view of the workflow.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs succeeded                  :</pre></th><td style="color: rgb(136, 136, 136);">the total number of succeeded jobs  during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs failed                      :</pre></th><td style="color: rgb(136, 136, 136);">the total number of failed jobs  during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs unsubmitted                 :</pre></th><td style="color: rgb(136, 136, 136);">the total number of unsubmitted jobs during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs unknown                     :</pre></th><td style="color: rgb(136, 136, 136);">the total number of unknown jobs during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># Total tasks succeeded            :</pre></th><td style="color: rgb(136, 136, 136);">the total number of succeeded tasks</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># Total tasks failed               :</pre></th><td style="color: rgb(136, 136, 136);">the total number of failed tasks</td></tr>
		</tbody>
		</table>
		</p> 
	
		<p>Selecting a particular run will take you the page which contains all the information about a workflow. The workflow page contains the following details. </p>

		<h3> Workflow execution details</h3>
		<p>For workflows
having sub workflow jobs (i.e SUBDAG and SUBDAX jobs), the sub workflow jobs are considered as single jobs.i.e The parent workflow won't recursively calculate sub workflows job information. </p>
		<table style="color: rgb(96, 0, 0);">
		<tbody><tr><th style="color: rgb(96, 0, 0);"><pre>Workflow runtime(min,sec)         :</pre></th><td style="color: rgb(136, 136, 136);">the walltime from the start of the workflow execution to the end as reported by the DAGMAN.In case of rescue dag the value is the cumulative of all retries.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre>Cumulative workflow runtime(min,sec):</pre></th><td style="color: rgb(136, 136, 136);">the sum of the walltime of all jobs as reported by the DAGMan .In case of job retries the value is the cumulative of all retries.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre>Total jobs run                     :</pre></th><td style="color: rgb(136, 136, 136);">the total number of jobs runs during the workflow run . In case of a failed workflow the number of jobs run could be less than the total jobs in the planned workflow. This is a runtime view of the workflow.</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs succeeded                  :</pre></th><td style="color: rgb(136, 136, 136);">the total number of succeeded jobs  during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs failed                      :</pre></th><td style="color: rgb(136, 136, 136);">the total number of failed jobs  during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs unsubmitted                 :</pre></th><td style="color: rgb(136, 136, 136);">the total number of unsubmitted jobs during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># jobs unknown                     :</pre></th><td style="color: rgb(136, 136, 136);">the total number of unknown jobs during the workflow run</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># Total tasks succeeded            :</pre></th><td style="color: rgb(136, 136, 136);">the total number of succeeded tasks</td></tr>
		<tr><th style="color: rgb(96, 0, 0);"><pre># Total tasks failed               :</pre></th><td style="color: rgb(136, 136, 136);">the total number of failed tasks</td></tr>
		</tbody>
		</table>
		
		<h3> Workflow execution environment</h3>
		<p>The workflow execution ennvironment contains the details in the braindump file. It contains information like dax label, dag label, submit dir, pegasus home environment variables etc.</p>

		<h3> Job statistics</h3>
		<p>Job statistics contains the following details about the jobs in  workflow.</p>
		<table style="color: rgb(96, 0, 0);">
<tbody>
<tr><th style="color: rgb(96, 0, 0);"><pre>Job                  :</pre></th><td style="color: rgb(136, 136, 136);">the name of the job</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Site                 :</pre></th><td style="color: rgb(136, 136, 136);">the site where job ran.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Kickstart(sec.)      :</pre></th><td style="color: rgb(136, 136, 136);">the actual duration of the job in seconds on the remote compute node. In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Post(sec.)           :</pre></th><td style="color: rgb(136, 136, 136);">the postscript time as reported by DAGMan .In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>DAGMan(sec.)         :</pre></th><td style="color: rgb(136, 136, 136);">the time between the last parent job of a job completes and the job gets submitted.In case of retries the value of the last retry is used for calculation.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>CondorQTime(sec.)    :</pre></th><td style="color: rgb(136, 136, 136);">the time between submission by DAGMan and the remote Grid submission. It is an estimate of the time spent in the condor q on the submit node .In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Resource(sec.)       :</pre></th><td style="color: rgb(136, 136, 136);">the time between the remote Grid submission and start of remote execution . It is an estimate of the time job spent in the remote queue .In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Runtime(sec.)        :</pre></th><td style="color: rgb(136, 136, 136);">the time spent on the resource as seen by Condor DAGMan . Is always >=kickstart .In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Seqexec(sec.)        :</pre></th><td style="color: rgb(136, 136, 136);">the time taken for the completion of a clustered job .In case of retries the value is the cumulative of all retries.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Seqexec-Delay(sec.)   :</pre></th><td style="color: rgb(136, 136, 136);">the time difference between the time for the completion of a clustered job and sum of all the individual tasks kickstart time .In case of retries the value is the cumulative of all retries.</td></tr>
</tbody>
</table>

		<h3> Task statistics</h3>
		<p>Task statistics contains the following details about the tranformation in  workflow.</p>
<table style="color: rgb(96, 0, 0);">
<tbody>
<tr><th style="color: rgb(96, 0, 0);"><pre>Transformation       :</pre></th><td style="color: rgb(136, 136, 136);"> name of the transformation.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Count                :</pre></th><td style="color: rgb(136, 136, 136);"> the number of times the transformation was executed.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Succeeded            :</pre></th><td style="color: rgb(136, 136, 136);"> the number of times the tranformation execution succeeded.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Failed               :</pre></th><td style="color: rgb(136, 136, 136);"> the number of times the tranformation execution failed.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Mean(sec.)           :</pre></th><td style="color: rgb(136, 136, 136);"> the mean of the transformation runtime.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Variance(sec.)       :</pre></th><td style="color: rgb(136, 136, 136);"> the variance of the transformation runtime.Variance is calculated using the on-line algorithm by Knuth (http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance).</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Min(sec.)            :</pre></th><td style="color: rgb(136, 136, 136);"> the minimum transformation runtime value.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Max(sec.)            :</pre></th><td style="color: rgb(136, 136, 136);"> the maximum transformation runtime value.</td></tr>
<tr><th style="color: rgb(96, 0, 0);"><pre>Total(sec.)          :</pre></th><td style="color: rgb(136, 136, 136);"> the cumulative of transformation runtime.</td></tr>
</tbody>
</table>
		<h3> Dax graph</h3>
		<p>Graph image of the dax file .</p>
		<h3> Dag graph</h3>
		<p>Graph image of the dag file .</p>
		<h3>Workflow execution gantt chart</h3>
		<p>The toolbar at the top provides zoom in/out , pan left/right/top/bottom and show/hide job name functionality. The toolbar at the bottom can be used to show/hide job states. A failed job is shown by a red border. Clicking on a sub workflow job will take you to the corresponding sub workflow.</p>
		<h3>Host over time chart</h3>
		<p>The toolbar at the top provides zoom in/out , pan left/right/top/bottom and show/hide host name functionality. The toolbar at the bottom can be used to show/hide job states. A failed job is shown by a red border. Clicking on a sub workflow job will take you to the corresponding sub workflow.</p>
		</center>
		</div>
</div> <!-- end of main div -->
<div id = "footer_div" class = "footer">
</div>
<?php  
	do_html_footer();
?>
