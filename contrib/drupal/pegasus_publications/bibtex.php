<?php
// Bibtex parser based on phpBibLib:
// https://people.mmci.uni-saarland.de/~jilles/prj/phpBibLib/
// Created by Jilles Vreeken http://www.adrem.ua.ac.be/~jvreeken/

class Bibtex {
    var $bibarr = array();

    function Bibtex($file) {
        if (!is_string($file)) {
            trigger_error("Bibtex: Expected file name to be string");
        }
        if (!is_file($file)) {
            trigger_error("Bibtex: Invalid file: " . $file);
        }

        // File is valid, parse it
        $this->ParseFile($file);

        // Now sort the references
        uasort($this->bibarr, 'Bibtex::CmpEntries');
    }

    function CmpEntries($a, $b) {
        // Sort by year, but if year is the same, sort by the original position
        // in the file.
        if (array_key_exists('year', $a)) {
            $yeara = $a['year'];
        } else {
            $yeara = 9999;
        }
        if (array_key_exists('year', $b)) {
            $yearb = $b['year'];
        } else {
            $yearb = 9999;
        }
        if ($yeara == $yearb) {
            return $a['position'] - $b['position'];
        } else {
            return $yearb - $yeara;
        }
    }

    function ParseFile($pathname) {
        $file = file($pathname);
        $rawblock = '';
        $inblock = false;

        foreach($file as $line) {
            $line = str_replace('\\textsc','', $line);
            $line = str_replace('\\textit','', $line);

            $pattern = "/\\$(.*)\\$/i";
            $replacement = '<span class=\'bibtex-mathmode\'>${1}</span>';
            $line = preg_replace($pattern, $replacement, $line);

            $pattern = "/\{\\\'(\w+)\}/";
            $replacement = '&${1}acute;';
            $line = preg_replace($pattern, $replacement, $line);
            $pattern = "/\\\'\{(\w+)\}/";
            $line = preg_replace($pattern, $replacement, $line);

            $pattern = '/\{\\\"(\w+)\}/';
            $replacement = '&${1}uml;';
            $line = preg_replace($pattern, $replacement, $line);
            $pattern = '/\\\"\{[\\\]?(\w+)\}/';
            $line = preg_replace($pattern, $replacement, $line);

            $replacement = '&ccedil;';
            $line = str_replace('\cc', $replacement, $line);
            $line = preg_replace($pattern, $replacement, $line);
            $pattern = '/\\\c\{c\}/';
            $line = preg_replace($pattern, $replacement, $line);

            $pattern = '/\{\\\noopsort\{\w+\}\}/';
            $line = preg_replace($pattern, '', $line);

            $pattern = '/=\s*"/';
            $line = preg_replace($pattern, '= {', $line);
            $line = str_replace('"','}', $line);

            $line = str_replace('\\delta','&delta;', $line);
            $line = str_replace('$','', $line);

            $seg = $line;

            // check if starts with @ ...
            $segT = trim($seg);
            if(strlen($segT) > 0 && $segT[0] == '}') {
                $rawblock .= ' '.trim(substr($seg,0,strpos($seg,'%%') == false ? strlen($seg) : strpos($seg,'%%')), " \n\r");
                $this->ParseEntry($rawblock);
                $rawblock = '';
                $inblock = false;
            } else if(strlen($segT) > 0 && $segT[0] == '@') {
                // starts something new
                $this->ParseEntry($rawblock);
                $rawblock = '';
                $inblock = true;
            }

            $pc = strpos($seg,'%%') == false ? strlen($seg) : strpos($seg,'%%');
            if($inblock === true)
                $rawblock .= ' '.trim(substr($seg,0,$pc), " \n\r");
        }

        if($rawblock != '') {
            $this->ParseEntry($rawblock);
        }
    }

    function ParseEntry($rb) {
        $rb = trim($rb);
        if($rb == '')
            return;

        $entry = array();

        $pa = strpos($rb, '{');
        $pc = strpos($rb, ',');
        if($pa == false) {
            return;
        }
        $entry['type'] = strtolower(substr($rb, 1, $pa-1));
        if($entry['type'] == 'preamble')
            return;
        $entry['key'] = Bibtex::NormalizeKey(substr($rb, $pa+1, $pc-$pa-1));

        $rb = substr($rb, $pc+1, -1);

        $rawfields = array();
        $start = 0;
        $nest = 0;
        for($i=0; $i < strlen($rb); $i++) {
            if($rb[$i] == ',' && $nest == 0) {
                $start = $i+1;
            } else if($rb[$i] == '{' || ($rb[$i] == '"' && ($i+1 < strlen($rb) && ($rb[$i+1] != ',' && $rb[$i+1] != "\n")))) {
                $nest++;
            } else if($rb[$i] == '}' || ($rb[$i] == '"' && (    ($i+1 < strlen($rb) && ($rb[$i+1] == ',' || $rb[$i+1] == "\n" || $rb[$i+1] == "\r")))
                                                                                                                ||
                                                                                                                ($i+1==strlen($rb)))
                                                                                                            ) {
                $nest--;

                if($nest == 0) {
                    $rawfields[] = trim(substr($rb, $start, $i-$start+1));
                    $start = $i+1;
                }
            }
        }
        foreach($rawfields as $field) {
            $fieldname = strtolower(trim(substr($field, 0, strpos($field, '='))));
            $fieldval = substr(trim(substr($field, strpos($field, '=')+1)),1,-1);
            $entry[$fieldname] = $fieldval;
        }

        // pre-handle author array
        if(isset($entry['author'])) {
            $entry['author'] = Bibtex::NormalizeAuthorStr($entry['author']);
        }

        // pre-handle editor array
        if(isset($entry['editor'])) {
            $entry['editor'] = Bibtex::NormalizeAuthorStr($entry['editor']);
        }

        // pre-handle pagefrom/to
        if(isset($entry['pages'])) {
            $pattern = '/^([0-9]+)[^0-9]+([0-9]+)$/';
            if(preg_match($pattern, $entry['pages'], $match))
                $entry['pages'] = $match[1] . '-' . $match[2];
        }

        // pre-handle booktitle
        if(isset($entry['booktitle'])) {
            $entry['booktitle'] = str_replace('{','', str_replace('}','',str_replace('`', '\'', $entry['booktitle'])));
        }

        // filter out {'s
        foreach($entry as $field => $val) {
            $val = str_replace('{', '', $val);
            $val = str_replace('}', '', $val);
            $entry[$field] = $val;
        }

        // Set the position
        $entry['position'] = count($this->bibarr);

        $this->bibarr[$entry['key']] = $entry;
    }

    function NormalizeKey($key) {
        return str_replace('/', '_', str_replace(':', '_', trim($key)));
    }

    function NormalizeAuthorStr($authorstr) {
        $aarr = explode(' and ', $authorstr);
        $aarr = array_map('trim', $aarr);
        for($i=0; $i < count($aarr); $i++) {
            if(strpos($aarr[$i], ',') == false) {
                // No comma? Then the first token is the first name, and the 
                // others are the last name
                $names = explode(" ", $aarr[$i]);
                $last = array_pop($names);
                $aarr[$i] = array(implode(" ", $names), $last);
            } else {
                // Comma? Then everything before the comma is the last name
                $na = explode(',', $aarr[$i]);
                $aarr[$i] = array($na[1], $na[0]);
            }
        }
        return $aarr;
    }

    function HasEntry($key) {
        $key = Bibtex::NormalizeKey($key);
        return isset($this->bibarr[$key]);
    }

    function GetEntry($key) {
        $key = Bibtex::NormalizeKey($key);
        return isset($this->bibarr[$key]) ? $this->bibarr[$key] : false;
    }

    function RenderError($msg) {
        return '<span class="bibtex-error">' . $msg . '</span>, ';
    }

    function RenderAuthors($authors) {
        $total = count($authors);
        $result = '';
        foreach($authors as $i => $author) {
            $a = implode(" ", $author);
            if ($i == 0) {
                $result = $a;
            }
            else if ($i + 1 == $total) {
                $result .= ' and ' . $a;
            }
            else {
                $result .= ', ' . $a;
            }
        }
        return $result;
    }

    function RenderCoins($entry) {
        $type = $entry["type"];

        $coins = array();
        $coins["ctx_ver"] = "Z39.88-2004";
        $coins["rfr_id"] = "info:sid/pegasus.isi.edu";

        if ($type === "article") {
            $coins["rft.genre"] = "article";
            $coins["rft_val_fmt"] = "info:ofi/fmt:kev:mtx:journal";
            $coins["rft.atitle"] = $entry["title"];
            $coins["rft.jtitle"] = $entry["journal"];
            if (array_key_exists("volume", $entry)) {
                $coins["rft.volume"] = $entry["volume"];
            }
            if (array_key_exists("number", $entry)) {
                $coins["rft.issue"] = $entry["number"];
            }
            if (array_key_exists("pages", $entry)) {
                $coins["rft.pages"] = $entry["pages"];
            }
        }
        else if ($type === "inproceedings") {
            $coins["rft_val_fmt"] = "info:ofi/fmt:kev:mtx:book";
            $coins["rft.genre"] = "proceeding";
            $coins["rft.atitle"] = $entry["title"];
            $coins["rft.btitle"] = $entry["booktitle"];
        }
        else if ($type === "inbook") {
            $coins["rft_val_fmt"] = "info:ofi/fmt:kev:mtx:book";
            $coins["rft.genre"] = "bookitem";
            $coins["rft.atitle"] = $entry["title"];
            $coins["rft.btitle"] = $entry["booktitle"];
            if (array_key_exists("publisher", $entry)) {
                $coins["rft.pub"] = $entry["publisher"];
            }
            if (array_key_exists("pages", $entry)) {
                $coins["rft.pages"] = $entry["pages"];
            }
        }
        else if ($type === "book") {
            $coins["rft_val_fmt"] = "info:ofi/fmt:kev:mtx:book";
            $coins["rft.genre"] = "book";
            $coins["rft.btitle"] = $entry["title"];
            $coins["rft.pub"] = $entry["publisher"];
        }
        else if ($type === "techreport") {
            $coins["rft_val_fmt"] = "info:ofi/fmt:kev:mtx:book";
            $coins["rft.genre"] = "report";
            $coins["rft.btitle"] = $entry["title"];
            $coins["rft.pub"] = $entry["institution"];
            $coins["rft.series"] = $entry["number"];
        }
        else {
            // Don't generate COinS markup for this entry
            return "";
        }

        $coins["rft.au"] = $entry["author"];

        $date = "";
        if (array_key_exists("month", $entry)) {
            $date .= $entry["month"] . " ";
        }
        $date .= $entry["year"];
        $coins["rft.date"] = $date;

        $result = "";
        foreach($coins as $key => $value) {
            if ($result != "") {
                $result .= "&";
            }

            // Reprocess the authors
            if ($key === "rft.au") {
                $authors = "";
                foreach($value as $author) {
                    if ($authors != "") {
                        $authors .= "&";
                    }
                    $authors .= "rft.au=" . urlencode($author[1] . ", " . $author[0]);
                }
                $result .= $authors;
            }
            else {
                $result .= $key . "=" . urlencode($value);
            }
        }

        return '<span class="Z3988" title="' . $result . '"></span>';
    }

    function RenderEntry($entry) {
        $result = Bibtex::RenderCoins($entry);

        $type = $entry["type"];

        if (array_key_exists("author", $entry)) {
            $result .= '<span class="bibtex-author">' . Bibtex::RenderAuthors($entry['author']) . '</span>, ';
        }

        if (array_key_exists("url", $entry)) {
            $result .= '<a href="' . $entry["url"] . '" class="bibtex-url">';
        }
        $result .= '<span class="bibtex-title">' . $entry["title"] . '</span>, ';
        if (array_key_exists("url", $entry)) {
            $result .= '</a>';
        }

        if ($type === "inproceedings") {
            if (array_key_exists("booktitle", $entry)) {
                $result .= '<span class="bibtex-conference">' . $entry["booktitle"] . '</span>, ';
            } else {
                $result .= Bibtex::RenderError('Conference missing');
            }
        }
        else if ($type === "article") {
            if (array_key_exists("journal", $entry)) {
                $result .= '<span class="bibtex-journal">' . $entry["journal"] . '</span>, ';
            } else {
                $result .= Bibtex::RenderError('Journal missing');
            }

            if (array_key_exists("volume", $entry)) {
                $result .= $entry["volume"];
                if (array_key_exists("number", $entry)) {
                    $result .= ':' . $entry["number"];
                }
                $result .= ', ';
            }

            if (array_key_exists("pages", $entry)) {
                $result .= ' pp. ' . $entry["pages"] . ', ';
            }

            if (array_key_exists("month", $entry)) {
                $result .= $entry["month"] . ', ';
            }

        }
        else if ($type === "techreport") {
            if (array_key_exists("institution", $entry)) {
                $result .= '<span class="bibtex-institution">' . $entry["institution"] . '</span>, ';
            }
            if (array_key_exists("number", $entry)) {
                $result .= $entry["number"] . ', ';
            }
        }
        else if ($type === "inbook" or $type === "book") {
            if ($type === "inbook") {
                if (array_key_exists("booktitle", $entry)) {
                    $result .= 'in <span class="bibtex-book">' . $entry["booktitle"] . '</span>, ';
                } else {
                    $result .= Bibtex::RenderError('Book missing');
                }
            }

            if (array_key_exists("editor", $entry)) {
                $result .= 'edited by ' . Bibtex::RenderAuthors($entry["editor"]) . ', ';
            }

            if (array_key_exists("publisher", $entry)) {
                $result .= '<span class="bibtex-publisher">' . $entry["publisher"] . '</span>, ';
            }
        }
        else {
            $result .= Bibtex::RenderError("Unknown Type");
        }

        if (array_key_exists("year", $entry)) {
            $result .= $entry["year"];
        } else {
            $result .= Bibtex::RenderError("Year Missing");
        }

        $result .= ".";

        if (array_key_exists("doi", $entry)) {
            $result .= ' <span class="bibtex-doi"><a href="http://dx.doi.org/' . $entry["doi"] . '">doi:' . $entry["doi"] . '</a></span>';
        }

        if (array_key_exists("note", $entry)) {
            $result .= ' <span class="bibtex-note">(' . $entry["note"] . ')</span>';
        }

        $result .= "\n";

        return $result;
    }

    function Render() {
        $result = "";
        $current_year = 9999;

        foreach($this->bibarr as $key => $entry) {

            // Print a heading for each year
            if (array_key_exists('year', $entry)) {
                if ($entry['year'] != $current_year) {
                    $current_year = $entry['year'];
                    $result .= '<h3 class="bibtex-year-heading">' . $current_year . '</h3>';
                }
            }

            $result .= '<p class="bibtex-entry">' . Bibtex::RenderEntry($entry) . '</p>';
        }

        return $result;
    }
}

?>

