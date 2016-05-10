#!/bin/bash

dir=$(cd $(dirname $0) && pwd)

PEGASUS_HOME=$(dirname $(dirname $(which pegasus-plan)))

cat > $dir/diamond.dax <<END
<?xml version="1.0" encoding="UTF-8"?>
<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd" version="3.4" name="diamond">
	<job id="ID0000001" name="preprocess">
		<argument>-i <file name="f.a"/> -o <file name="f.b1"/> -o <file name="f.b2"/></argument>
		<uses name="f.b2" link="output" register="false" transfer="false"/>
		<uses name="f.b1" link="output" register="false" transfer="false"/>
		<uses name="f.a" link="input"/>
	</job>
	<job id="ID0000002" name="findrange">
		<argument>-i <file name="f.b1"/> -o <file name="f.c1"/></argument>
		<uses name="f.b1" link="input"/>
		<uses name="f.c1" link="output" register="false" transfer="false"/>
	</job>
	<job id="ID0000003" name="findrange">
		<argument>-i <file name="f.b2"/> -o <file name="f.c2"/></argument>
		<uses name="f.c2" link="output" register="false" transfer="false"/>
		<uses name="f.b2" link="input"/>
	</job>
	<job id="ID0000004" name="analyze">
		<argument>-i <file name="f.c1"/> -i <file name="f.c2"/> -o <file name="f.d"/></argument>
		<uses name="f.c2" link="input"/>
		<uses name="f.d" link="output" register="false" transfer="true"/>
		<uses name="f.c1" link="input"/>
	</job>
	<child ref="ID0000002">
		<parent ref="ID0000001"/>
	</child>
	<child ref="ID0000003">
		<parent ref="ID0000001"/>
	</child>
	<child ref="ID0000004">
		<parent ref="ID0000002"/>
		<parent ref="ID0000003"/>
	</child>
</adag>
END

cat > $dir/f.a <<END
This is the input file for the workflow
END

cat > $dir/rc.dat <<END
f.a    file://$dir/f.a    pool="local"
END

cat > $dir/sites.xml <<END
<?xml version="1.0" encoding="UTF-8"?>
<sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd" version="4.0">

    <!-- The local site contains information about the submit host -->
    <!-- The arch and os keywords are used to match binaries in the transformation catalog -->
    <site handle="local" arch="x86_64" os="LINUX">

        <!-- These are the paths on the submit host were Pegasus stores data -->
        <!-- Scratch is where temporary files go -->
        <!--
        <directory type="shared-scratch" path="$dir/run">
            <file-server operation="all" url="file://$dir/run"/>
        </directory>
        -->
        <!-- Storage is where pegasus stores output files -->
        <directory type="local-storage" path="$dir/outputs">
            <file-server operation="all" url="file://$dir/outputs"/>
        </directory>
    </site>


    <!-- This site is a Condor pool running on localhost. Normally this site would 
         have many different machines in it, but for this tutorial it is just a 
         "Personal Condor" pool. Really, the local site and PegasusVM are the same,
         we just distinguish between them in this tutorial for illustrative purposes. -->
    <site handle="pegasus" arch="x86_64" os="LINUX">

        <!-- These are the paths on the PegasusVM site where Pegasus stores data -->
        <!-- Scratch is where Pegasus stores intermediate files -->
        <directory type="shared-scratch" path="$dir/work">
            <file-server operation="all" url="scp://127.0.0.1$dir/work"/>
        </directory>

        <!-- These profiles tell Pegasus that the PegasusVM site is a plain Condor pool -->
        <profile namespace="pegasus" key="style">condor</profile>
        <profile namespace="condor" key="universe">vanilla</profile>

        <!-- This profile tells Pegasus where the worker package is installed on PegasusVM -->
        <profile namespace="env" key="PEGASUS_HOME">$PEGASUS_HOME</profile>

        <!-- This profile tells Pegasus where to find the user's private key for SCP transfers -->
        <profile namespace="pegasus" key="SSH_PRIVATE_KEY">$HOME/.ssh/id_rsa_nopass</profile>
    </site>
</sitecatalog>
END

cat > $dir/pegasus.conf <<END
# This tells Pegasus where to find the Site Catalog
pegasus.catalog.site.file=sites.xml

# This tells Pegasus where to find the Replica Catalog
pegasus.catalog.replica=File
pegasus.catalog.replica.file=rc.dat

# This tells Pegasus where to find the Transformation Catalog
pegasus.catalog.transformation=Text
pegasus.catalog.transformation.file=tc.dat
END

cat > $dir/tc.dat <<END
tr preprocess {
    site pegasus {
        pfn "$dir/bin/preprocess"
        arch "x86_64"
        os "linux"
        type "INSTALLED"
    }
}

tr findrange {
    site pegasus {
        pfn "$dir/bin/findrange"
        arch "x86_64"
        os "linux"
        type "INSTALLED"
    }
}

tr analyze {
    site pegasus {
        pfn "$dir/bin/analyze"
        arch "x86_64"
        os "linux"
        type "INSTALLED"
    }
}
END

if [ -z "$SHADOWQ_AMQP_PASSWORD" ]; then
    echo "Set SHADOWQ_AMQP_PASSWORD"
    exit 1
fi

export SHADOWQ_PROVISIONER_INTERVAL=30
export SHADOWQ_ESTIMATES=$dir/estimates.txt
export SHADOWQ_MAKESPAN=300
export SHADOWQ_AMQP_URL="amqps://gideon:$SHADOWQ_AMQP_PASSWORD@gaul.isi.edu:5671/%2F"
export SHADOWQ_SLICEID="test"

pegasus-plan --conf pegasus.conf -d diamond.dax --dir submit \
	--force --sites pegasus -o local --cleanup none --submit "$@"

