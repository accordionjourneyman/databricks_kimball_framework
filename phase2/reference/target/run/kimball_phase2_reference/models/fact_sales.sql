
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
      
  

  

  merge into dbt_reference.fact_sales as DBT_INTERNAL_DEST
      using fact_sales__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.source_system = DBT_INTERNAL_DEST.source_system
               and 
                  DBT_INTERNAL_SOURCE.line_id = DBT_INTERNAL_DEST.line_id
              

      when matched then update set
         * 

      when not matched then insert *
