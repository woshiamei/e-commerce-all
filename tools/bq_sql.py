from reco.utils.connect import BQ_Client

bq_client = BQ_Client()


def run():

    sql = """
      SELECT fullVisitorId,product.productSKU,date,
          sum(CASE WHEN product.isImpression IS NULL THEN 0 ELSE 1 END) as p_view,--曝光
          sum(CASE WHEN product.isClick IS NULL THEN 0 ELSE 1 END) as p_click ,--点击
        sum(case when cast(hits.eCommerceAction.action_type as int64)=3 then 1 else 0 end ) as is_addtocart,--加购
        sum(case when cast(hits.eCommerceAction.action_type as int64)=6 then 1 else 0 end ) as is_order --购买
          from `truemetrics-164606.116719673.ga_sessions_*`,
          unnest(hits) as hits,
          unnest(hits.product) as product 
          where _TABLE_SUFFIX BETWEEN '20220713' AND '20220713'
          group by fullVisitorId,product.productSKU,date
          having p_click>0 or is_addtocart>0 or is_order>0
          order by fullVisitorId, date
    """

    print('query....')
    bqListRet = bq_client.query(sql)
    print('turn to list....')
    bqListRet = [list(i) for i in bqListRet]
    print('total prod`uct num is {}'.format(len(bqListRet)))


if __name__ == '__main__':
    run()