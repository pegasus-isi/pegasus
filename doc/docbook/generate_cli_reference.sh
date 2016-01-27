#!/bin/bash

cat <<END
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE chapter PUBLIC "-//OASIS//DTD DocBook XML V4.5//EN"
"http://www.oasis-open.org/docbook/xml/4.5/docbookx.dtd">
<chapter id="cli" xmlns:xi="http://www.w3.org/2001/XInclude">
  <title id="pegasus-cli-chapter">Command Line Tools</title>
END

for f in $(ls manpage.*.xml); do
    echo "  <xi:include href=\"$f\"/>"
done

echo "</chapter>"
