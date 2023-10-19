import os
import geopandas as gpd
from shapely import wkb
from datetime import datetime



def esri_to_gdf(aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    elif '.gdb' in aoi:
        l = aoi.split('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename=gdb, layer=fc)
    else:
        raise Exception('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def reproject_to_bcalbers(gdf):
    """ Reprojects a gdf to bc albers"""
    if gdf.crs != 'epsg:3005':
        gdf = gdf.to_crs('epsg:3005')
    
    return gdf


def flatten_to_2d(gdf):
    """Flattens 3D geometries to 2D"""
    for i, row in gdf.iterrows():
        geom = row.geometry
        if geom.has_z:
            geom_2d = wkb.loads(wkb.dumps(geom, output_dimension=2))
            gdf.at[i, 'geometry'] = geom_2d
    
    return gdf


def prepare_geo_data(aoi):
    """ Runs data preparation functions"""
    gdf = esri_to_gdf(aoi)
    gdf = reproject_to_bcalbers(gdf)
    gdf = flatten_to_2d(gdf)
    
    return gdf


def add_new_mpa(mpas_wks, new_mpa, name_col, extra_txt):
    dfo_mpas = os.path.join(mpas_wks, 'DFO_MPA_MPO_ZPM.gdb', 'DFO_MPA_MPO_ZPM')
    gdf_dfo_mpas = prepare_geo_data(dfo_mpas)
    gdf_new_mpa = prepare_geo_data(new_mpa)
    
    gdf_new_mpa.rename(columns={name_col:'NAME_E'}, inplace=True)
    gdf_new_mpa = gdf_new_mpa[['NAME_E', 'geometry']]
    
    if extra_txt:
        gdf_new_mpa['NAME_E']= gdf_new_mpa['NAME_E'] + ' ' + extra_txt
    
    gdf = gdf_dfo_mpas.append(gdf_new_mpa)
    
    today = datetime.today().strftime("%Y%m%d")
    out_shp= os.path.join(mpas_wks, 'MPAs_updates', today + 'updated_MPAs.shp')

    gdf.to_file(out_shp)
    
    

if __name__ == "__main__":
    mpas_wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\DFO\MPAs'
    new_mpa= os.path.join(mpas_wks, 'addition_oct2023', 'ProposedCaamanobndry_GAC_20230420.shp')
    
    name_col= 'UID'
    extra_txt= 'Caamano Sound'
    gdf= add_new_mpa(mpas_wks, new_mpa, name_col, extra_txt)