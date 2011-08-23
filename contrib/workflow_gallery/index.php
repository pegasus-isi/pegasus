<?php  
	 include_once( $_SERVER['DOCUMENT_ROOT']."/static/includes/common.inc.php" );
	 do_html_header("Documentation");
?>
<div id = "header_div" class ="header">
	<h1>Workflow Gallery</h1>
</div>
<div id="main" class ="columns">
	<div id="left_div" class ="left">
	<a href ="help.php">Gallery Info</a>
	</div>
	<div id="right_div" class ="right" >
	</div>
	<div id="center_div" class ="middle">
		<center>
		<table border="1">
			<tr>
				<th>Workflow type</th>
				<th>Structure</th>
			</tr>
			
			<tr>
				<td>
					<p>
					<h3><a href ="gallery/broadband/index.php">Broadband</a></h3>
					<pre>
Broadband platform enables researchers to combine
long period (<1.0Hz) deterministic seismograms 
with high frequency (~10Hz) stochastic seismograms.
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >					
						<img src="images/broadband.jpg" align="bottom" width="200"  /> 
					</div>
				</td>
			</tr>

			<tr>
				<td>
					<p>
					<h3><a href ="gallery/cybershake/index.php">CyberShake</a></h3>
					<pre>
The CyberShake workflow is used
by the Southern Calfornia Earthquake
Center to characterize
earthquake hazards in a region.
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >					
						<img src="images/cybershake.jpg" align="bottom" width="200"  /> 
					</div>
				</td>
			</tr>
			
			<tr>
				<td>
					<p>
					<h3><a href ="gallery/epigenomics/index.php">Epigenomics </a></h3>
					<pre>
The epigenomics workflow created
by the USC Epigenome Center
and the Pegasus Team is used to
automate various operations
in genome sequence processing.
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >					
						<img src="images/epigenomics.jpg"  align="bottom" width="200"  /> 
					</div>
				</td>
			</tr>
			
			<tr>
				<td>
					<p>
					<h3><a href ="gallery/ligo/index.php">LIGO</a></h3>
					<pre>
LIGO workflow is used to generate and
analyze gravitational waveforms
from data collected during the
coalescing of compact binary systems. 
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >
						<img src="images/ligo.jpg"  align="bottom" width="200"  />
					</div>
				</td>
			</tr>

			<tr>
				<td>
					<p>
					<h3><a href ="gallery/montage/index.php">Montage</a></h3>
					<pre>
The Montage application created
by NASA/IPAC stitches together multiple
input images to create
custom mosaics of the sky.  
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >
						<img src="images/montage.jpg"  align="bottom" width="200"  />
					</div>
				</td>
			</tr>
	
			<tr>
				<td>
					<p>
					<h3><a href ="gallery/periodogram/index.php">Periodogram</a></h3>
					<pre>
NASA’s Infared Processing and Analysis 
Center (IPAC) use workflow technologies to 
process the large amount of data produced 
by the Kepler mission. IPAC has developed
a set of analysis codes to compute periodograms
from light curves. These periodograms reveal 
periodic signals in the light curves that arise 
from transiting planets and stellar variability.
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >
						<img src="images/periodogram.jpg"  align="bottom" width="200"  />
					</div>
				</td>
				</tr>

			<tr>
				<td>
					<p>
					<h3><a href ="gallery/proteomics/index.php">Proteomics</a></h3>
					<pre>
Scientists at OSU use Pegasus for 
mass-spectrometry-based proteomics. 
Proteomics workflows have been 
executed on local clusters and cloud resources. 
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >
						<img src="images/proteomics.jpg"  align="bottom" width="200"  />
					</div>
				</td>
			</tr>

			<tr>
				<td>
					<p>
					<h3><a href ="gallery/sipht/index.php">Sipht</a></h3>
					<pre>
The SIPHT workflow, from the
bioinformatics project at Harvard,
is used to automate the search for
untranslated RNAs (sRNAs) for bacterial
replicons in the NCBI database.  
					</pre>
					</p>
				</td>
				<td>
					<div class = "gallery_image" >
						<img src="images/sipht.jpg"  align="bottom" width="200"  />
					</div>
				</td>
			</tr>


		</table>
		</center>
		</div>
</div> <!-- end of main div -->
<div id = "footer_div" class = "footer">
</div>
<?php  
	do_html_footer();
?>
