#!/usr/bin/env python
"""
Pegasus utility for generating breakdown chart


"""
from __future__ import absolute_import

##
#  Copyright 2010-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision$
import os
import sys
import logging

logger = logging.getLogger(__name__)

from Pegasus.tools import utils
from Pegasus.plots_stats import utils as plot_utils
from . import populate
from datetime import timedelta
from datetime import datetime


#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
output_dir = None


#----------print workflow details--------
def print_workflow_details(workflow_stat , output_dir):
	"""
	Prints the data required for generating the gantt chart into data file.
	@param workflow_stat the WorkflowInfo object reference 
	@param output_dir output directory path
	"""
	trans_info =  "var bc_data = [" + workflow_stat.get_formatted_transformation_data() + "];"
	
	# print javascript file
	data_file = os.path.join(output_dir,  "bc_" + workflow_stat.wf_uuid+"_data.js")
	try:
		fh = open(data_file, "w")
		fh.write( "\n")
		fh.write(trans_info)
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()	
	return
	


def create_action_script(output_dir):
	"""
	Generates the action script file which contains the javascript functions used by the main html file.
	@param output_dir output directory path
	"""
	action_content = """
function printTransformationDetails(d){
	var transformation_details = "name : "+d.name ;
	transformation_details +=  "\\nTotal count : "+ d.count ;
	transformation_details +=  "\\nSucceeded count : "+ d.success ;
	transformation_details +=  "\\nFailed count : "+d.failure ;
	transformation_details +=  "\\nMin Runtime : "+d.min ;
	transformation_details +=  "\\nMax Runtime : "+d.max ;
	transformation_details +=  "\\nAvg Runtime : "+d.avg ;
	transformation_details +=  "\\nTotal Runtime : "+d.total ;
	alert(transformation_details);
}

function setBreakdownBy(isBreakdown){
	
	breakdownByCount= isBreakdown;
	loadBCGraph();
}


function setBCChartTitle(){
	if(breakdownByCount){
		return "Invocation breakdown by count grouped by transformation name";
	}else{
		return "Invocation breakdown by runtime grouped by transformation name";
	}
	
}

function loadBCGraph(){
	
	bc_headerPanel.render();
	bc_chartPanel.render();
	bc_chartPanel.def("o", -1); 
}


function getOuterAngle(d){
	if(breakdownByCount){
		return d.count/ bc_total_count * 2 * Math.PI;
	}
	else{
		return d.total/ bc_total_runtime * 2 * Math.PI;
	}
}

function getInnerRadius(d){
	if(d.failure < 0){
			return 0;
	}
	
	if(breakdownByCount){
		return bc_radius*(d.failure/d.count);
	}
	else{
 		// Changed to fix JIRA issue PM-566
		return bc_radius*(d.failure/d.count);
		// return bc_radius*d.total*(d.failure/d.count);
	}
}


"""
	# print action script
	data_file = os.path.join(output_dir,  "bc_action.js")
	try:
		fh = open(data_file, "w")
		fh.write( action_content)
		fh.write( "\n")
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()



def create_header(workflow_stat):
	"""
	Generates the header html content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	header_str = """
<html>
<head>
<title>"""+ workflow_stat.wf_uuid +"""</title>
<style type ='text/css'>
#breakdown_chart{
border:2px solid orange;
}
#breakdown_chart_footer_div{
border:2px solid  #C35617;
border-top-style:none;
}
#breakdown_chart_legend_div{
color:#0066CC;
}
.header_level1{
font-family:"Times New Roman", Times, serif; 
font-size:36px;
}
.header_level2{
font-family:"Times New Roman", Times, serif; 
font-size:30px;
padding-top:25px;
}
</style>
</head>
<body>
<script type='text/javascript' src='js/protovis-r3.2.js'>
</script>
"""
	header_str += plot_utils.create_home_button()
	return header_str
	

def create_toc(workflow_stat):
	"""
	Generates the table of content for the pages
	@param workflow_stat the WorkflowInfo object reference 
	"""
	toc_str ="""
<div class ='header_level1'>Invocation breakdown chart </div>
	"""
	toc_str += """
<a href ='#chart_div'>Invocation breakdown chart</a><br/>
<a href ='#env_div'> Workflow environment</a><br/>
	"""
	if len(workflow_stat.sub_wf_id_uuids) >0:
		toc_str += """
<a href ='#sub_div'> Sub workflows</a><br/>
""" 
	return toc_str
	
def create_include(workflow_stat):
	"""
	Generates the html script include content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	include_str = """
<script type='text/javascript' src='bc_action.js'>
</script>
<script type='text/javascript' src='bc_""" + workflow_stat.wf_uuid  +"""_data.js'>
</script>
"""
	return include_str
	
def create_variable(workflow_stat):
	"""
	Generates the javascript variables used to generate the chart.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	number_of_invocations , total_runtime = workflow_stat.get_total_count_run_time()
	# Adding  variables
	var_str = """
<script type='text/javascript'>
var bc_w = 860;
var bc_h = 400;
var bc_radius = 120;
var bc_centerX = bc_w/2;
var bc_centerY = bc_h/2;
var bc_headerPanelWidth =  bc_w;
var bc_headerPanelHeight  = 100 ;
var bc_total_count  = """ + str(number_of_invocations) +""";
var bc_total_runtime =  """ + str(total_runtime)+""";
var bc_footerPanelWidth =  bc_w;
var bc_footerPanelHeight  =""" + str(30 + len(workflow_stat.transformation_statistics_dict)/4*15)  + """;
var bc_label_padding = 30
var bc_xLabelPos = bc_label_padding;
var bc_yLabelPos = 30;
var bc_labelWidth =200;
var breakdownByCount = true;
</script>
	"""
	return var_str
	

def create_toolbar_panel(workflow_stat):
	"""
	Generates the top level toolbar content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str = """
<script type="text/javascript+protovis">
var bc_headerPanel = new pv.Panel()
.width(bc_headerPanelWidth)
.height(bc_headerPanelHeight)
.fillStyle('white');
bc_headerPanel.add(pv.Label)
.top(40)
.left( bc_label_padding)
.font(function() {return 24 +'px sans-serif';})
.textAlign('left')
.textBaseline('bottom')
.text(function(){ return setBCChartTitle();});

bc_headerPanel.add(pv.Label)
	.top(80)
	.left(bc_label_padding)
	.font(function() {return 16 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
	"""
	panel_str += ".text('" +workflow_stat.dax_label + "');\n"
	panel_str += """
bc_headerPanel.render();

</script>	
"""
	return panel_str

def create_chart_panel(workflow_stat):
	"""
	Generates the chart panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="""
<script type="text/javascript+protovis">
var bc_chartPanel = new pv.Panel()
.width(bc_w)
.height(bc_h);

bc_chartPanel.def("o", -1); 

var outerWedge = bc_chartPanel.add(pv.Wedge)
.left(bc_centerX)
.bottom(bc_centerY);

outerWedge.data(bc_data)
.outerRadius(bc_radius)
.angle(function(d){return getOuterAngle(d);})
.left(function() bc_centerX
	+ Math.cos(this.startAngle() + this.angle() / 2)
	* ((this.parent.o() == this.index) ? 10 : 0))
.bottom(function() bc_centerY
	- Math.sin(this.startAngle() + this.angle() / 2)
	* ((this.parent.o() == this.index) ? 10 : 0))
.event("mouseover", function() this.parent.o(this.index))
.event("click", function(d) printTransformationDetails(d))
.fillStyle(function(d)d.color);


var innerWedge = bc_chartPanel.add(pv.Wedge)
.data(bc_data)
.left(bc_centerX)
.bottom(bc_centerY)
.outerRadius( function(d){return getInnerRadius(d);})
.angle(function(d) {return getOuterAngle(d);})
.left(function() bc_centerX
+ Math.cos(this.startAngle() + this.angle() / 2)
* ((this.parent.o() == this.index) ? 10 : 0))
.bottom(function() bc_centerY
- Math.sin(this.startAngle() + this.angle() / 2)
* ((this.parent.o() == this.index) ? 10 : 0))
.event("mouseover", function() this.parent.o(this.index))
 .event("click", function(d) printTransformationDetails(d))   
.fillStyle("red");
bc_chartPanel.render();
</script>
	"""
	return panel_str
	

def create_legend_panel(workflow_stat):
	"""
	Generates the bottom level legend panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="""
<script type="text/javascript+protovis">
var bc_footerPanel = new pv.Panel()
.width(bc_footerPanelWidth)
.height(bc_footerPanelHeight)
.fillStyle('white');
bc_footerPanel.add(pv.Dot)
.data(bc_data)
.left( function(d){
	if(this.index == 0){
		bc_xLabelPos = bc_label_padding;
		bc_yLabelPos = bc_footerPanelHeight - 15 ;
	}else{
		if(bc_xLabelPos + bc_labelWidth > bc_w - (bc_label_padding + bc_labelWidth)){
			bc_xLabelPos =  bc_label_padding;
			bc_yLabelPos -=15;
		}
		else{
			bc_xLabelPos += bc_labelWidth;
		}
	}
	return bc_xLabelPos;}
)
.bottom(function(d){
	return bc_yLabelPos;}
)
.fillStyle(function(d) d.color)
.strokeStyle(null)
.size(49)
.event("click", function(d) printTransformationDetails(d))
.anchor('right').add(pv.Label)
.textMargin(6)
.textAlign('left')
.text(function(d) d.name);

bc_footerPanel.render();
</script>
"""
	return panel_str


def create_bottom_toolbar():
	"""
	Generates the bottom toolbar html content.
	"""
	toolbar_content ="""
<div id ='breakdown_chart_footer_div' style='width: 860px; margin : 0 auto;' >
	<div>
		<div>Breakdown  by</div>
		<input type='radio' name='by_filter' value='by count' onclick="setBreakdownBy(true);" checked/> count<br />
		<input type='radio' name='by_filter' value='by runtime' onclick="setBreakdownBy(false);"/> runtime<br />
	</div>
	<div id = 'breakdown_chart_legend_div'>
	<p><b>Note</b>: Legends can be clicked to find information corresponding to the transformation name.</p>
	</div>
</div>
	"""
	return toolbar_content


def create_breakdown_plot(workflow_info , output_dir):
	"""
	Generates the html page content for displaying the breakdown chart.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""
	print_workflow_details(workflow_info ,output_dir)
	str_list = []
	wf_content = create_include(workflow_info)
	str_list.append(wf_content)
	# Adding  variables
	wf_content =create_variable(workflow_info)
	str_list.append(wf_content)
	wf_content = """
<div id ='breakdown_chart' style='width: 860px; margin : 0 auto;' >
	"""
	str_list.append(wf_content)
	# adding the tool bar panel
	wf_content =create_toolbar_panel(workflow_info)
	str_list.append(wf_content)
	# Adding the chart panel
	wf_content =create_chart_panel(workflow_info)
	str_list.append(wf_content)
	# Adding the legend panel
	wf_content =create_legend_panel(workflow_info)
	str_list.append(wf_content)
	wf_content = """
</div>
"""
	str_list.append(wf_content)
	wf_content =create_bottom_toolbar()
	str_list.append(wf_content)
	return "".join(str_list)
		
		
	

def create_breakdown_plot_page(workflow_info ,output_dir):
	"""
	Prints the complete html page with the gantt chart and workflow details.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""	
	str_list = []
	wf_page = create_header(workflow_info)
	str_list.append(wf_page)
	wf_page = """
<center>
	"""
	str_list.append(wf_page)
	wf_page = create_toc(workflow_info)
	str_list.append(wf_page)
	wf_page = """<div id ='chart_div' class ='header_level2'> Invocation breakdown chart </div>"""
	str_list.append(wf_page)
	wf_page = create_breakdown_plot(workflow_info ,output_dir)
	str_list.append(wf_page)
	# printing the brain dump content
	wf_page = """<div id ='env_div' class ='header_level2'> Workflow environment </div>"""
	str_list.append(wf_page)
	if workflow_info.submit_dir is None:
		logger.warning("Unable to display brain dump contents. Invalid submit directory for workflow  " + workflow_info.wf_uuid)
	else:
		wf_page = plot_utils.print_property_table(workflow_info.wf_env,False ," : ")
		str_list.append(wf_page)
	# print sub workflow list
	if len(workflow_info.sub_wf_id_uuids) >0:
		wf_page = """<div id ='sub_div' class ='header_level2'> Sub workflows </div>"""
		str_list.append(wf_page)
		wf_page = plot_utils.print_sub_wf_links(workflow_info.sub_wf_id_uuids)
		str_list.append(wf_page)
	wf_page = """
</center>
	"""
	str_list.append(wf_page)
	wf_page = """
<div style='clear: left'>
</div>
</body>
</html>
	"""
	str_list.append(wf_page)
	data_file = os.path.join(output_dir,  workflow_info.wf_uuid+".html")
	try:
		fh = open(data_file, "w")
		fh.write( "\n")
		fh.write("".join(str_list))	
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()	
	return
def setup(submit_dir,out_dir,env):
	"""
	Setup the pegasus breakdown module
	@param submit_dir submit directory path 
	@out_dir the output directory path
	@env the environment variables
	"""
	# global reference
	global output_dir
	output_dir = out_dir
	utils.create_directory(output_dir)
	src_js_path = env['pegasus_javascript_dir'] 
	src_img_path = os.path.join(env['pegasus_share_dir']  , "plots/images/protovis/")
	dest_js_path = os.path.join(output_dir, "js")
	dest_img_path = os.path.join(output_dir, "images/")
	utils.create_directory(dest_js_path)
	utils.create_directory(dest_img_path)
	plot_utils.copy_files(src_js_path , dest_js_path)
	plot_utils.copy_files(src_img_path, dest_img_path)
	# copy images from common
	src_img_path = os.path.join(env['pegasus_share_dir']  , "plots/images/common/")
	plot_utils.copy_files(src_img_path, dest_img_path) 	 	
	create_action_script(output_dir)



def generate_chart(workflow_info):
	"""
	Generates the breakdown chart and all it's required files
	@workflow_info WorkflowInfo object reference
	"""
	create_breakdown_plot_page(workflow_info , output_dir)


# ---------main----------------------------------------------------------------------------
def main():
	sys.exit(0)


if __name__ == '__main__':
	main()
