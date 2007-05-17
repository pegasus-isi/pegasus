Sub UserGuideUpdateTOC()
'
' UserGuideUpdateTOC Macro
' Macro recorded 10/11/2005 by Mike Wilde
'
    ActiveWindow.ActivePane.View.Type = wdMasterView
    ActiveDocument.Subdocuments.Expanded = Not ActiveDocument.Subdocuments. _
        Expanded
    Selection.WholeStory
    Selection.Fields.Update
    ActiveDocument.Save
    Application.Quit SaveChanges:=wdSaveChanges
End Sub

Sub UserGuideSaveAsHTML()
    Dim strDocName As String
    Dim intPos As Integer

    'Find position of extension in filename
    
    strDocName = ActiveDocument.FullName
    intPos = InStrRev(strDocName, ".")

    'Strip off extension and add ".html" extension
        
    strDocName = Left(strDocName, intPos - 1)
    'strDocName = ".\" & strDocName & ".html"
    'strDocName = "C:\Mike\vds\cvs-doc\vds\doc\ug4\" & strDocName & ".html"
    strDocName = strDocName & ".html"

    'Save file as HTML with new extension
    
    ActiveDocument.SaveAs FileName:=strDocName, _
        FileFormat:=wdFormatFilteredHTML
    Application.Quit SaveChanges:=wdDoNotSaveChanges
End Sub
