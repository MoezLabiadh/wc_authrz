import os
import arcpy

file_nbr = sys.argv[1]
legal_txt = sys.argv[2]
scale = sys.argv[3]
workspace = sys.argv[4]

mxd = arcpy.mapping.MapDocument("CURRENT")
df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

arcpy.AddMessage ('Setting Scale')
df.scale = int(scale)

layersList = arcpy.mapping.ListLayers(mxd,"",df)
tenure_layer = layersList[1]

arcpy.AddMessage ('Updating the Def Query')
defQuery = """CROWN_LANDS_FILE = '{}'""".format (file_nbr)
tenure_layer.definitionQuery = defQuery

arcpy.AddMessage ('Updating the Legal Description Text')
elm = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "Legal_desc")[0]
elm.text = legal_txt

mxd.dataDrivenPages.refresh ()
#arcpy.RefreshActiveView()
#arcpy.RefreshTOC()
#mxd.save()



arcpy.AddMessage ('Exporting Maps')

for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
    mxd.dataDrivenPages.currentPageID = pageNum
    arcpy.AddMessage( "..Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
    output = os.path.join(workspace, 'LDS_{0}_{1}.pdf'.format(file_nbr, str(pageNum)))
    arcpy.mapping.ExportToPDF(mxd,output)

arcpy.AddMessage  ('Completed! Check the output folder for results')
