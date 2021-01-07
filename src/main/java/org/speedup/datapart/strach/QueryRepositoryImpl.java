package org.speedup.datapart.strach;

import com.leadmap.cloud.datagis.AppConfig;
import com.leadmap.cloud.datagis.model.DBConfiguration;
import com.leadmap.cloud.datagis.model.queryVO.*;
import com.leadmap.cloud.datagis.utils.GeometryConverter;
import com.leadmap.cloud.datagis.utils.SQLStringUtil;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.*;

import static com.leadmap.cloud.datagis.service.QueryDao.*;

@Repository("QueryRepository")
public class QueryRepositoryImpl implements QueryRepository {
    public static final String IllegalArgumentExceptionStringforGISSTATISTICS = "postgresql 要求选择列表必须在 groupby字句中或者对其使用聚合函数";
    private static   final Logger LOG = LoggerFactory.getLogger(QueryRepositoryImpl.class);
    private static DBConfiguration dbInfo=null;

    private  void navieMethod(FieldDescriptionResult descriptionResult){
        FieldDescriptionVO fieldDescriptionVO = new FieldDescriptionVO();
        descriptionResult.setResult(fieldDescriptionVO);
        try {
            Class.forName("org.postgis.DriverWrapper");
            String url = dbInfo.toString();
//            System.out.println(url);

//            String url= String.format("jdbc:postgresql_postGIS://%s:%d/%s", dbInfo.getHost(),dbInfo.getPort(),dbInfo.getDbName());
            Connection connection = DriverManager.getConnection(url, dbInfo.getUser(), dbInfo.getPassword());
            Statement statement = connection.createStatement();
            List<String> tables = Arrays.asList("devicedescription", "fieldcategory", "fielddescription");
            for(String tabName:tables) {
                List<Map<String,String>> result =new ArrayList<>();
                ResultSet resultSet = statement.executeQuery("select * from "+tabName);
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
//                FieldDescriptionVO vo =new FieldDescriptionVO();
                    Map<String, String> vo = new HashMap<>();
                    for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                        vo.put(metaData.getColumnName(i), resultSet.getString(i));
                    }
                    result.add(vo);
                }
                if(tabName.equalsIgnoreCase("devicedescription")){
                    fieldDescriptionVO.setDevicedescription(result);
                }else if(tabName.equalsIgnoreCase("fieldcategory")){
                    fieldDescriptionVO.setFieldcategory(result);
                }else {
                    fieldDescriptionVO.setFielddescription(result);
                }
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            descriptionResult.failed(e.getMessage());
        }
    }

    public  FieldDescriptionResult getFieldDescription(DBConfiguration dbConfiguration){
       FieldDescriptionResult result =new FieldDescriptionResult();

       FieldDescriptionVO vo =new FieldDescriptionVO();
       if(!dbConfiguration.equals(dbInfo)){
            dbInfo=dbConfiguration;
       }

        navieMethod(result);
        return  result;
    }





    @Override
    public GisObjectsQueryResult regularQuery(DBConfiguration dbConfiguration, List<String> fieldsLists, List<String> table_names, String whereStr, com.leadmap.cloud.datagis.model.feature.Geometry geometryModel, boolean fieldsOnly) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        GisObjectsQueryResult queryResult = new GisObjectsQueryResult();
        queryResult.setStatus(true);
        List<SimpleFeatureVO> features =new ArrayList<>();
        queryResult.setResults(features);
        SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
        for(String tableName:table_names){
            //只有fieldlist不为空添加
            if(!fieldsOnly&&!CollectionUtils.isEmpty(fieldsLists)) {
                fieldsLists.add("geom");
            }
            selectSQL.setTabName(tableName).setSelectedFields(fieldsLists);
            //空间字段不为空且whereStr不为空
            if(geometryModel!=null){
                Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                selectSQL.setGeomStr(geometry.toText());
            }
             //设置where条件
            if(!StringUtils.isEmpty(whereStr)) selectSQL.setWhereStr(whereStr);

            String sqlTemplate = selectSQL.toString();
            LOG.info(sqlTemplate);
            ExecuteJDBCRegularQuery(sqlTemplate,queryResult,tableName,fieldsOnly);
        }
        queryResult.setTotalCount(queryResult.getResults().size());
        return queryResult;
    }

    @Override
    public GisObjectsQueryResult sortingAndPagingQuery(DBConfiguration dbConfiguration,int pageStart, int pageSize, List<String> fieldsLists, List<String> table_names, String whereStr, com.leadmap.cloud.datagis.model.feature.Geometry geometryModel, boolean fieldsOnly) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        GisObjectsQueryResult queryResult = new GisObjectsQueryResult();
        queryResult.setStatus(true);
        List<SimpleFeatureVO> features =new ArrayList<>();
        queryResult.setResults(features);
        SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
        for(String tableName:table_names){
            //只有fieldlist不为空添加
            if(!fieldsOnly&&!CollectionUtils.isEmpty(fieldsLists)) {
                fieldsLists.add("geom");
            }
            selectSQL.setTabName(tableName).setSelectedFields(fieldsLists);
            //空间字段不为空且whereStr不为空
            if(geometryModel!=null){
                Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                selectSQL.setGeomStr(geometry.toText());
            }
            //设置where条件
            if(!StringUtils.isEmpty(whereStr)) selectSQL.setWhereStr(whereStr);

            selectSQL.setLimit(pageSize).setOffset(pageStart);
            String sqlTemplate = selectSQL.toString();
            LOG.info(sqlTemplate);
            ExecuteJDBCRegularQuery(sqlTemplate,queryResult,tableName,fieldsOnly);
        }
        GisCountResult totalCount = getTotalCount(dbConfiguration, table_names, whereStr, geometryModel);
        queryResult.setTotalCount(totalCount.getCount());
        return queryResult;
    }

    @Override
    public GisCountResult getTotalCount(DBConfiguration dbConfiguration, List<String> fcName, String whereStr, com.leadmap.cloud.datagis.model.feature.Geometry geometryModel) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        List<SimpleFeatureVO> features =new ArrayList<>();
        GisCountResult gisCountResult = new GisCountResult();
        long count=0;
        GisObjectsQueryResult queryResult = new GisObjectsQueryResult();
        queryResult.setResults(features);
        gisCountResult.setStatus(true);
        SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
        for(String tableName:fcName){

            selectSQL.setTabName(tableName).setSelectedFields(Arrays.asList("COUNT(*) as count"));
            //空间字段不为空且whereStr不为空
            if(geometryModel!=null){
                Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                selectSQL.setGeomStr(geometry.toText());
            }
            //设置where条件
            if(!StringUtils.isEmpty(whereStr)) selectSQL.setWhereStr(whereStr);
            String sqlTemplate = selectSQL.toString();
            LOG.info(sqlTemplate);
            ExecuteJDBCRegularQuery(sqlTemplate,queryResult,tableName,true);
            if(!queryResult.isStatus()){
                gisCountResult.failed(queryResult.getMsg());
                break;
            }else{
                if (queryResult.getResults().size()!=0) {
                    String count1 = queryResult.getResults().get(0).getProperties().get("count");
                    count=+Integer.valueOf(count1);
                    queryResult.getResults().clear();//清空本轮结果
                }
            }
        }
        gisCountResult.setCount(count);

        return gisCountResult;
    }

    @Override
    public GISFieldQueryResult getQualifiedIdList(DBConfiguration dbConfiguration, List<String> fcName, String whereStr, com.leadmap.cloud.datagis.model.feature.Geometry geometryModel) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        GISFieldQueryResult gisFieldQueryResult = new GISFieldQueryResult();
        List<Long> idList =new ArrayList<>();
        gisFieldQueryResult.setFieldLists(idList);
        SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
        for(String tableName:fcName){

            selectSQL.setTabName(tableName).setSelectedFields(Arrays.asList("OBJECTID as  ID"));
            //空间字段不为空且whereStr不为空
            if(geometryModel!=null){
                Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                selectSQL.setGeomStr(geometry.toText());
            }
            //设置where条件
            if(!StringUtils.isEmpty(whereStr)) selectSQL.setWhereStr(whereStr);
            String sqlTemplate = selectSQL.toString();
            LOG.info(sqlTemplate);
            ExecuteJDBCFieldsQuery(sqlTemplate,gisFieldQueryResult);
        }


        return gisFieldQueryResult;

    }

    @Override
    public GISFieldQueryResult getDistinctFieldsList(DBConfiguration dbConfiguration, List<String> fcName, String whereStr, com.leadmap.cloud.datagis.model.feature.Geometry geometryModel, String fieldName) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        GISFieldQueryResult gisFieldQueryResult = new GISFieldQueryResult();
        List<String> idList =new ArrayList<>();
        gisFieldQueryResult.setFieldLists(idList);
        SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
        if (fcName!=null) {
            for(String tableName:fcName){

                selectSQL.setTabName(tableName).setSelectedFields(Arrays.asList(" DISTINCT "+fieldName));
                //空间字段不为空且whereStr不为空
                if(geometryModel!=null){
                    Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                    selectSQL.setGeomStr(geometry.toText());
                }
                //设置where条件
                if(!StringUtils.isEmpty(whereStr)) selectSQL.setWhereStr(whereStr);
                String sqlTemplate = selectSQL.toString();
                LOG.info(sqlTemplate);
                ExecuteJDBCFieldsQuery(sqlTemplate,gisFieldQueryResult);
            }
        }
        return gisFieldQueryResult;
    }



    /**
     * @param alias 传入别名
     * @return 返回fieldName,相当于主键
     */
    private String getAliasFieldName(String alias,Map<String,GISStatisticsItem> fields){
        for (Map.Entry<String, GISStatisticsItem> gisStatisticsItemEntry : fields.entrySet()) {
            GISStatisticsItem value = gisStatisticsItemEntry.getValue();
            if (!StringUtils.isEmpty(value.getFieldAlias())&&value.getFieldAlias().equalsIgnoreCase(alias)) {
                return gisStatisticsItemEntry.getKey();
            }
        }

        return "";
    }
    private List<String> ValidateOrderItem(List<OrderByItem> orderFields,  Map<String,String> aliasMap, Map<String,GISStatisticsItem> fieldMap,List<String> groupByFields) {
        //保存验证过的orderBy 字段
        List<String> orderFilters = new ArrayList<>();

        //order by 字句的字段必须在groupBy 子句或者 聚合函数的alias中
        if(orderFields!=null&&orderFields.size()!=0) {
            for (OrderByItem orderByFiledItem : orderFields) {
                String orderByFiled = orderByFiledItem.getField();
                //                    check orderByField is in aliasMap or groupByFields
                //                aliasMap.get(orderByFiled).
                //检查是否在 aliasMap中：
                //case1 : 在alias Map 中,使用别名 OK
                //case2: alias Map获取为空，没有设置 alias 使用原列名 ，当仅当 原列名的聚合类型为 NONE 时可以使用
                if (
                        aliasMap.get(orderByFiled) != null ||////case1 : 在alias Map 中,使用别名 OK
                                (fieldMap.get(orderByFiled)!=null&&fieldMap.get(orderByFiled).getStatisticsType() == StatisticsType.NONE)
                ) {
                    String temp1 = orderByFiled + (orderByFiledItem.isUseDesc() ? " DESC " : " ASC ");
                    orderFilters.add(temp1);

                } else {
                    throw new IllegalArgumentException(String.format("orderByFields  [\"%s\"] should be contained in  statItems  alias [%s] or in groupByFields  [%s] otherwise the sql execution will failed", orderByFiled,
                            String.join(",", aliasMap.values()),
                            String.join(",", groupByFields)
                    ));
                }
            }//end of for
        }
        return orderFilters;
    }


    @Override
    public GisStatisticsResult getAggregationFields(DBConfiguration dbConfiguration,
                                                    Map<Integer, String> classCode2Fcname, GISStatisticsItemList lists,
                                                    com.leadmap.cloud.datagis.model.feature.Geometry geometryModel,
                                                    String whereStr, List<String> groupByFields, List<OrderByItem> orderFields) {
        if(DBConfiguration.isValid(dbConfiguration)){
            QueryDao.SetDBConfiguration(dbConfiguration);
        }
        GisStatisticsResult gisStatisticsResult = new GisStatisticsResult();
        List<GisStatisticsResultItem> items = new ArrayList<>();
        gisStatisticsResult.setResults(items);
        for (Map.Entry<Integer, String> entry : classCode2Fcname.entrySet()) {
            GisStatisticsResultItem item = new GisStatisticsResultItem();
            //分类码
            Integer classCode = entry.getKey();
            String tableName = entry.getValue();
            item.setClassCode(classCode);
            SQLStringUtil.SelectSQL selectSQL = new SQLStringUtil.SelectSQL();
            selectSQL.setTabName(tableName);
            selectSQL.setWhereStr("classcode="+classCode);
            //StatisticsItem  获取聚集字段
            List<String> aggregationFields =new ArrayList<>();
            Map<String,String> aliasMap= new HashMap<>();
            Map<String,GISStatisticsItem> fieldMap =new HashMap<>();
            if(lists!=null && lists.size()!=0) {
                for(GISStatisticsItem gisStatisticsItem:lists) {
                    String fieldName = gisStatisticsItem.getFieldName();
                    aliasMap.put(fieldName, gisStatisticsItem.getFieldAlias()==null?gisStatisticsItem.getFieldName():gisStatisticsItem.getFieldAlias());
                    fieldMap.put(fieldName, gisStatisticsItem);
                    switch (gisStatisticsItem.getStatisticsType()) {
                        case SUM:
                            String s1 = String.format("SUM(%s) %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                            aggregationFields.add(s1);
                            break;
                        case NONE:
                            if (groupByFields != null && !groupByFields.contains(gisStatisticsItem.getFieldName())) {
                                throw new IllegalArgumentException(QueryRepositoryImpl.IllegalArgumentExceptionStringforGISSTATISTICS);
                            } else {
                                String s2 = String.format("%s %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                                aggregationFields.add(s2);
                            }
                            break;
                        case MAX:
                            String s3 = String.format("MAX(%s) %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                            aggregationFields.add(s3);
                            break;
                        case MIN:
                            String s4 = String.format("MIN(%s) %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                            aggregationFields.add(s4);
                            break;
                        case COUNT:
                            String s5 = String.format("COUNT(%s) %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                            aggregationFields.add(s5);
                            break;
                        case AVG:
                            String s6 = String.format("AVG(%s) %s", gisStatisticsItem.getFieldName(), StringUtils.isEmpty(gisStatisticsItem.getFieldAlias()) ? "" : " as " + gisStatisticsItem.getFieldAlias());
                            aggregationFields.add(s6);
                            break;
                        default:
                            continue;
                    }
                }
                selectSQL.setSelectedFields(aggregationFields);
                if(geometryModel!=null){
                    Geometry geometry = GeometryConverter.ConvertVOToJts(geometryModel);
                    selectSQL.setGeomStr(geometry.toText());
                }

                selectSQL.AddWhereStr(whereStr);
                if(groupByFields!=null&&groupByFields.size()!=0){
                    selectSQL.setGroupByFields(groupByFields);
                }
                if(orderFields!=null) {
                    try {
                        List<String> orderFilters = ValidateOrderItem(orderFields, aliasMap, fieldMap, groupByFields);
                        selectSQL.setOrderByFields(orderFilters);
                    } catch (Exception e) {
                        e.printStackTrace();
                        gisStatisticsResult.failed(e.getMessage());
                        return gisStatisticsResult;
                    }
                }
                String sqlTemplate = selectSQL.toString();
                LOG.info(sqlTemplate);
                ExecuteStatisticsQuery(sqlTemplate,classCode,gisStatisticsResult);
            }

        }
        return gisStatisticsResult;
    }

    @Override
    public GisObjectsQueryResult getGeometryOperation(GeoCalculationType type, List<com.leadmap.cloud.datagis.model.feature.Geometry> geometries, double radius) {
        GisObjectsQueryResult result=new GisObjectsQueryResult();
        List<SimpleFeatureVO> simpleFeatureVOS =new ArrayList<>();
        result.setResults(simpleFeatureVOS);
        switch (type){
            case BUFFER:
            case BUFFERS:
                for (com.leadmap.cloud.datagis.model.feature.Geometry geometry : geometries) {
                    Geometry geom = GeometryConverter.ConvertVOToJts(geometry);
                    Geometry buffer = geom.buffer(radius/AppConfig.OGC_DEGREE_TO_METERS);
                    SimpleFeatureVO featureVO = new SimpleFeatureVO();
                    featureVO.setGeometry(GeometryConverter.ConvertJTSToVO(buffer));
                    simpleFeatureVOS.add(featureVO);
                }
                break;
            case INTERSECTS: {
                if (geometries.size() <= 1) throw new RuntimeException("geometry at least two to do this operations");
                Geometry left = GeometryConverter.ConvertVOToJts(geometries.get(0));
                for (int i = 1; i < geometries.size(); ++i) {
                    Geometry right = GeometryConverter.ConvertVOToJts(geometries.get(i));
                    left = left.intersection(right);
                }
                SimpleFeatureVO featureVO = new SimpleFeatureVO();
                featureVO.setGeometry(GeometryConverter.ConvertJTSToVO(left));
                simpleFeatureVOS.add(featureVO);
                break;
            }
            case CONVEX_HULL: {
                if (geometries.size() < 1) throw new RuntimeException("geometry at least one to do this operations");
                Geometry left = GeometryConverter.ConvertVOToJts(geometries.get(0));
                for (int i = 1; i < geometries.size(); ++i) {
                    Geometry right = GeometryConverter.ConvertVOToJts(geometries.get(i));
                    left = left.union(right);
                }
                left=left.convexHull();
                SimpleFeatureVO featureVO = new SimpleFeatureVO();
                featureVO.setGeometry(GeometryConverter.ConvertJTSToVO(left));
                simpleFeatureVOS.add(featureVO);
                break;
            }
            case UNION:{
                if (geometries.size() <= 1) throw new RuntimeException("geometry at least two to do this operations");
                Geometry left = GeometryConverter.ConvertVOToJts(geometries.get(0));
                for (int i = 1; i < geometries.size(); ++i) {
                    Geometry right = GeometryConverter.ConvertVOToJts(geometries.get(i));
                    left = left.union(right);
                }
                SimpleFeatureVO featureVO = new SimpleFeatureVO();
                featureVO.setGeometry(GeometryConverter.ConvertJTSToVO(left));
                simpleFeatureVOS.add(featureVO);
                break;
            }
            case BUFFER_UNION:{
                if (geometries.size() <= 1) throw new RuntimeException("geometry at least two to do this operations");
                Geometry left = GeometryConverter.ConvertVOToJts(geometries.get(0)).buffer(radius);
                for (int i = 1; i < geometries.size(); ++i) {
                    Geometry right = GeometryConverter.ConvertVOToJts(geometries.get(i));
                    left = left.union(right).buffer(radius/AppConfig.OGC_DEGREE_TO_METERS);
                }
                SimpleFeatureVO featureVO = new SimpleFeatureVO();
                featureVO.setGeometry(GeometryConverter.ConvertJTSToVO(left));
                simpleFeatureVOS.add(featureVO);
                break;
            }
        }

        return result;
    }

    @Override
    public GeoMeasureResult getGeometryProperty(GeoPropertyType type, List<com.leadmap.cloud.datagis.model.feature.Geometry> geometries) {
        GeoMeasureResult result =new GeoMeasureResult();
        List<Double> measures =new ArrayList<>();
        result.setResults(measures);
        boolean flag =(type==GeoPropertyType.SIZE)?true:false;
        try{
            for (com.leadmap.cloud.datagis.model.feature.Geometry geometry : geometries) {
                if(!flag) {
                    double length = GeometryConverter.ConvertVOToJts(geometry).getLength();
                    measures.add(length);
                }else{
                    double area = GeometryConverter.ConvertVOToJts(geometry).getArea();
                    measures.add(area);
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
            result.failed(ex.getMessage());
        }

        return result;
    }
}
