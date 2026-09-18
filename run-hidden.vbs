' run-hidden.vbs - launch a command with NO visible window (used by server-control)
' Usage: wscript.exe //B //Nologo run-hidden.vbs <arg1> [arg2] ...
' Each argument is quoted and joined into one command line, then run hidden.
Dim sh, i, cmd
Set sh = CreateObject("WScript.Shell")
cmd = ""
For i = 0 To WScript.Arguments.Count - 1
  If i > 0 Then cmd = cmd & " "
  cmd = cmd & Chr(34) & WScript.Arguments(i) & Chr(34)
Next
If cmd <> "" Then sh.Run cmd, 0, False
