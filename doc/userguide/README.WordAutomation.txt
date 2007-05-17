Userguide document automation


Create Single PDF File

Ensure PDF Options are set as follows:
- PDF Options to include Bookmarks,etc.
- Don't start Acrobat?


- Set View to Outline
- Expand Subdocuments
- set View to Print Layout
- set Display for Review to Final
- select all: ^A
- update selected fields: F9
- Menu: Adobe PDF > Convert to Adobe PDF
  - respond Yes to "vds-1.4Save Files?"
  - Set File to vds-1.4.userguide.pdf
  - respond Yes to "File Exists, Overwrite"
  - Respond Yes to "Turn off Tagging" ?

(dont retain accessibility features - its way too slow for debugging
the macro; can consider turning On once we get tthis working well.

Create HTML File Set


MS WORD COMMAND LINE INTERFACE (from MS Word 2003 Help):

To modify how Microsoft Word starts on a one-time basis, you can add
switches to the Microsoft Windows Run command (Start menu). If you
plan to use a modified startup method frequently, you can create a
shortcut on the Windows desktop.

Add switches to the Run command

On the Windows Start menu, click Run. 

Enter the path to Word, such as C:\Program Files\Microsoft
Office\Office\Winword.exe, or click Browse to locate it. At the end of
the path, type a space, and then type a startup switch.

Create a desktop shortcut

Right-click the Windows desktop, point to New, and then click
Shortcut. In the Type the location of the item box, enter the path to
Word, such as C:\Program Files\Microsoft Office\Office\Winword.exe, or
click Browse to locate it. At the end of the path, type a space, and
then type a startup switch.

Startup switches

/safe		Start Word in Office Safe Mode.

/ttemplatename	Start Word with a new document based on a template
other than the Normal template (Normal template: A global template
that you can use for any type of document. You can modify this
template to change the default document formatting or
content.). Example: /tMyfax.dot Note If the file name has spaces in
it, enclose the complete name in quotation marks — for example,
/t"Elegant Report.dot"

Security: Because templates can store macro viruses, be careful about
opening them or creating files based on new templates. Take the
following precautions: run up-to-date antivirus software on your
computer, set your macro security level to high, clear the Trust all
installed add-ins and templates check box, use digital signatures, and
maintain a list of trusted sources.

/pxslt Start Word with a new XML document based on the specified
Extensible Stylesheet Language Transformation (XSLT) (XSL
Transformation (XSLT): A file that is used to transform XML documents
into other types of documents, such as HTML or XML. It is designed for
use as part of XSL.). Example: /p:c:\MyTransform.xsl

/a Start Word and prevent add-ins (add-in: A supplemental program that
adds custom commands or custom features to Microsoft Office.) and
global templates (including the Normal template) from being loaded
automatically. The /a switch also locks the setting files.

/laddinpath Start Word and then load a specific Word add-in. Example:
/lSales.dll

Security: Use caution when running executable files or code in macros
or applications. Executable files or code can be used to carry out
actions that might compromise the security of your computer and data.

/m Start Word without running any AutoExec macros (macro: An action or
a set of actions you can use to automate tasks. Macros are recorded in
the Visual Basic for Applications programming language.).
 
/mmacroname Start Word and then run a specific macro. The /m switch
also prevents Word from running any AutoExec macros. Example:
/mSalelead

Security Because macros can contain viruses, be careful about running
them. Take the following precautions: run up-to-date antivirus
software on your computer; set your macro security level to high;
clear the Trust all installed add-ins and templates check box; use
digital signatures; maintain a list of trusted publishers.

/n Start a new instance of Word with no document open. Documents
opened in each instance of Word will not appear as choices in the
Window menu of other instances.

/w Start a new instance of Word with a blank document. Documents
opened in each instance of Word will not appear as choices in the
Window menu of the other instances.

Note To suppress automatic macros without using switches, hold down
SHIFT while you start Word.
