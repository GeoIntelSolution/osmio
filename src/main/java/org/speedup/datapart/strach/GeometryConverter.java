package org.speedup.datapart.strach;

import com.google.gson.Gson;
import com.leadmap.cloud.datagis.model.queryVO.SimpleFeatureVO;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.postgis.PGgeometry;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

public class GeometryConverter {
    //数据库Geometry转换为JTS Geometry 用于计算
    public static  org.locationtech.jts.geom.Geometry ConvertPGToJTSSGeometry(PGgeometry pGgeometry){
        if (pGgeometry==null) {
            return null;
        }

        try {
            WKTReader wktReader = new WKTReader();
            org.locationtech.jts.geom.Geometry read = wktReader.read(pGgeometry.toString());
            return read;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    //将JTS的Geometry转为DTO中的Geometry，用于返回


    public static com.leadmap.cloud.datagis.model.feature.Geometry ConvertJTSToVO(Geometry jtsGeometry){
        StringWriter sw =new StringWriter();
        try {
            //精度丢失问题
//            GeoJSON.write(jtsGeometry,sw);
            new GeoJsonWriter().write(jtsGeometry,sw);
            SimpleFeatureVO simpleFeatureVO = new SimpleFeatureVO();
            simpleFeatureVO.SetGeometry(sw.toString(),jtsGeometry.getGeometryType().toString());
            return simpleFeatureVO.getGeometry();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Geometry ConvertVOToJts(com.leadmap.cloud.datagis.model.feature.Geometry vo){
        String s = new Gson().toJson(vo);
        StringReader sw =new StringReader(s);
        try {
            Geometry read = new GeometryJSON().read(sw);
            return read;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static com.leadmap.cloud.datagis.model.feature.Geometry ConvertStringToGeometry(String geojson){
        StringReader stringReader = new StringReader(geojson);
        GeometryJSON geometryJSON = new GeometryJSON();
        SimpleFeatureVO featureVO = new SimpleFeatureVO();
        try {
            Geometry geometry = geometryJSON.read(stringReader);
            featureVO.SetGeometry(geojson,geometry.getGeometryType());
            return featureVO.getGeometry();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
