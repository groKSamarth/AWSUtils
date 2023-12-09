#fixed_width_spec
"""
{
  "column1": {
    "start": 1,
    "end": 5,
    "len": 5
  },
  "column2": {
    "start": 6,
    "end": 13,
    "len": 8
  },
  "column3": {
    "start": 14,
    "end": 23,
    "len": 10
  },
  "column4": {
    "start": 24,
    "end": 26,
    "len": 3
  }
  ...more columns as needed
}
"""

def generate_custom_grok_pattern(fixed_width_spec):
    try:
        logFormat, customPatterns = "", ""
        
        for column in fixed_width_spec: 
            logFormat+= f"%{{GET{column['name']}:{column['name']}}}"
            customPatterns+= f"GET{column['name']} ([^*]{{{column['len']}}})\n"

        logFormat+= "%{GREEDYDATA:extras}"
        
        print(logFormat)
        print(customPatterns)
        return logFormat, customPatterns
    
    except Exception as e:
        print("Exception raised in generate_custom_grok_pattern()", e)
        raise e
    

def read_grok_parsed_dynamic_frame(glueContext, s3uri, logFormat, customPatterns):
    try:
        grokDynamicFrame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3uri]},
            format="GrokLog",
            format_options={
                "logFormat": logFormat,
                "customPatterns": customPatterns
                            }
        )
        #"logFormat": "%{GETEMPID:EMPID}%{GETNAME:Name}%{GETDOB:DOB}%{GETCOMPANY:Company}%{GREEDYDATA:extras}",
        #"customPatterns": "GETEMPID ([^*]{5})\nGETNAME ([^*]{8})\nGETDOB ([^*]{5})\nGETCOMPANY ([^*]{7})"
        #"customPatterns": {'GETNAME': '([^*]{8})', 'GETEMPID': '([^*]{8})'} #%{GETEXTRAS:GreedyData #\nGETEXTRAS ([^.*$])
        
        return grokDynamicFrame
    
    except Exception as e:
        print("Exception raised in read_grok_parsed_dynamic_frame()", e)
        raise e
