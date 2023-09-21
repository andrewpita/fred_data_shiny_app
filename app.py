# The shinyswatch package provides themes from https://bootswatch.com/

import shinyswatch
from shiny import App, Inputs, Outputs, Session, render, ui, reactive, req
import pandas as pd
from datetime import datetime

####### everything in the app_ui section relates to the ui side #########################
#########################################################################################
app_ui = ui.page_navbar(
    # Available themes:
    #  cerulean, cosmo, cyborg, darkly, flatly, journal, litera, lumen, lux,
    #  materia, minty, morph, pulse, quartz, sandstone, simplex, sketchy, slate,
    #  solar, spacelab, superhero, united, vapor, yeti, zephyr
    shinyswatch.theme.darkly(),
    ui.nav(
        "",
        ####################### sidebar #####################################################
        ####################################################################################
        ui.layout_sidebar(
            ui.panel_sidebar(
                ui.tags.h3('Input Data'),
                ui.input_file("baseline_files", "Baseline Data: Upload one or more files here:",accept=[".csv"], multiple=True),
                ui.tags.h3("FRED Series Data"),
                ui.tags.br(),
                ui.navset_tab(
                ##################### The sidebar has three tabs, one for each of ##############################
                ##################### the methods of getting Series IDs for the purpose ########################
                ##################### of running the analysis #################################################
                    ui.nav(
                        "Category Based Search",
                        ui.tags.br(),
                        ui.output_ui("top_level_dropdown"),
                        ui.output_ui("dropdown_level_2"),
                        ui.output_ui("dropdown_level_3"),
                        ui.input_action_button("begin_series_search", "Start Series API Query",width="30%",class_="btn-primary")
                        ),
                    ui.nav(
                        "URL Based Web Scrape",
                        ui.tags.br(),
                        ui.input_text("scrape_url", "Enter a FRED URL:"),
                        ui.input_action_button("web_scrape_begin","Start Web Scrape",class_="btn-primary")
                    ),
                    ui.nav(
                        "Upload a List of FRED Series",
                        ui.tags.br(),
                        ui.input_file("series_file", "Upload a file with a list of FRED series:")
                    )
                ),
                ################## end sidebar tabs. everything below this is #########################
                ################## present on the sidebar no matter which tab #########################
                ################## is selected #######################################################
            
                ui.tags.h3("Analysis Parameters"),
                ui.input_slider("pct_change", "Set the interval for " + "%" + " change calculations", min = 0,max = 4, value=1),
                ui.tags.p("Note: For quarterly data, a value of 1 yields a quarter over quarter percent change. A value of 4 yields, year over year percent change." + 
                          "A value of 0 will result in the raw values being used."),
                ui.input_slider("lag",  "Set the number of lagging periods that will be applied to the baseline data.", min = 0,max = 10, value=5),
                ui.input_action_button("analysis_begin", "Begin Analysis", width="30%",class_="btn-primary")

                ################# end side bar ###############################################################
                #############################################################################################
            ),
            ################## main panel ###################################################################
            #################################################################################################
            ui.panel_main(
                ui.navset_tab(
                    ui.nav(
                        "Set Up",
                        ui.output_ui("baseline_data_preview"),
                        #ui.output_data_frame("table"),
                        ui.output_ui("series_table_section")
                    ),
                    ui.nav("Results",
                           ui.output_ui("results_table_section"),
                           ui.input_slider("pearson_thresh", "Select a Pearson correlation threshold", min=0, max=1, value=0.65),
                           ui.input_slider("spearman_thresh", "Select a Spearman correlation threshold", min=0, max=1, value=0.65)
                           )
                )
            )
            #################### end main panel ##################################################################
            #####################################################################################################
        ),
    ),
    ####################### page title ##############################################################
    title="FRED Data Mining Tool"
)

################ the server() functions creates the server and contains all the methods and logic ###############
################ that occur server side ########################################################################
def server(input: Inputs, output: Outputs, session: Session):

    ############ read in data to populate the dropdowns in the category search tab #########################
    ############ top_level categories populate the first dropdown ########################################
    ########### The dropdowns are hierarchical, so the one below depends on those above ##################
    categories_df = pd.read_csv("categories.csv")
    top_level_categories = categories_df[categories_df['parent_id'] == 0]

    ### reactive values respond to changes the user makes ###########################################
    ### this one waits for the user to input baseline data ##########################################
    ### and then stores it in a dictionary within a list to be displayed ###########################
    baseline_df_list = reactive.Value([])

    #### server logic to handle the file upload and prepare it for display ##################
    @reactive.Effect
    @reactive.event(input.baseline_files)
    def upload_contents():
        for i in input.baseline_files():
            df = pd.read_csv(i['datapath'])
            if isinstance(df.iloc[0,1],str):
                df = df.replace('%', '', regex=True)
                df.iloc[:,1] = pd.to_numeric(df.iloc[:,1])
            else:
                pass
            df = df.iloc[:,0:2]
            baseline_df_list.set(baseline_df_list() + [{i['name']:df}])
        baseline_df_list.set([{k: v for d in baseline_df_list() for k, v in d.items()}])

    ### server logic to create the data table displaying the uploaded #######################
    ### baseline data ###################################
    @output
    @render.data_frame
    @reactive.event(baseline_df_list,input.base_display,input.is_quarterly)
    def input_data_table():
        req(input.baseline_files())
        return render.DataTable(
                baseline_df_list()[0][input.base_display()],
                height=500,
                width="100%",
                filters = False
        )
    
    ### since multiple baseline data sets can be uploaded, the logic to create ###############
    ### the display part of the UI is handled by the server. This chuck creates the ##########
    ### dropdown to select which baseline data is displayed, along with the button #########
    ### that will execute date normalization ################################################
    @output
    @render.ui
    @reactive.event(baseline_df_list)
    def baseline_data_preview():
        req(baseline_df_list())

        return ui.TagList(
                ui.tags.h4("Uploaded Baseline Data"),
                ui.row(
                     ui.input_select('base_display',"Baseline file to display",list(baseline_df_list()[0].keys())),
                     #ui.input_checkbox("is_quarterly", "Is this data quarterly? If yes, click here to normalize the dates to match FRED.", value=False)
                     ui.input_action_button("is_quarterly", "Is this data quarterly? If yes, click here to normalize the dates to match FRED.",
                                            class_="btn-primary",width = "30%")

                ),
                ui.output_data_frame("input_data_table")
        )
    
    #### server logic to perform data normalization upon button press ###################3
    @reactive.Effect
    @reactive.event(input.is_quarterly)
    def date_normalize():
        df = baseline_df_list()[0][input.base_display()]
        row_count = df.shape[0]
        df['Date']  = df['Date'].astype(str)
        day = '01'
        new_date = []
        for i in range(0,row_count):
            if df.loc[i,'Date'][5:7] == '01' or df.loc[i,'Date'][5:7] == '02' or df.loc[i,'Date'][5:7] == '03':
                year = df.loc[i,'Date'][0:4]
                month = '-01-'
                new_date.append(year + month + day)
            elif df.loc[i,'Date'][5:7] == '04' or df.loc[i,'Date'][5:7] == '05' or df.loc[i,'Date'][5:7] == '06':
                year = df.loc[i,'Date'][0:4]
                month = '-04-'
                new_date.append(year + month + day)
            elif df.loc[i,'Date'][5:7] == '07' or df.loc[i,'Date'][5:7] == '08' or df.loc[i,'Date'][5:7] == '09':
                year = df.loc[i,'Date'][0:4]
                month = '-07-'
                new_date.append(year + month + day)
            elif df.loc[i,'Date'][5:7] == '10' or df.loc[i,'Date'][5:7] == '11' or df.loc[i,'Date'][5:7] == '12':
                year = df.loc[i,'Date'][0:4]
                month = '-10-'
                new_date.append(year + month + day)
            else:
                pass
        df['Date'] = new_date
        #df['Date'] = pd.to_datetime(df['Date'],format="%Y-%m-%d")
        baseline_df_list()[0][input.base_display()] = df

    #### server logic to create top dropdown ################
    @output
    @render.ui
    def top_level_dropdown():
        return(ui.input_selectize("top_dropdown", "Select a top level category:", top_level_categories['name'].values.tolist(),multiple = True))
    
    ### reactive value to store the IDs of the FRED category names selected ######
    ### in the top dropdown #######################################
    top_level_id_list = reactive.Value([])

    #### server logic to get the categry IDs for the names of #####
    ##### selected FRED categories ###############################33

    @reactive.Effect
    @reactive.event(input.top_dropdown)
    def add_top_level_ids():
        top_ids = categories_df[(categories_df['name'].isin(input.top_dropdown())) & (categories_df['parent_id'] == 0)]
        top_level_id_list.set(top_ids['id'].values.tolist())
    
    ### based on the top level IDs create the second level ########
    ### category dropdown ########################################

    @output
    @render.ui
    @reactive.event(top_level_id_list)
    def dropdown_level_2():

        level_2_categories = categories_df[categories_df["parent_id"].isin(top_level_id_list())]

        return(ui.input_selectize('dropdown2', 'Select 2nd level categories',level_2_categories['name'].values.tolist(),multiple=True))
    
    ### reactive value to store the IDs from the FRED categories #####
    ### selected  by 2nd dropdown ##############3
    level_2_id_list = reactive.Value([])

    ### server logic to get selected 2nd dropdown category IDs #####
    @reactive.Effect
    @reactive.event(input.dropdown2, top_level_id_list)
    def add_level_2_ids():
        level_2_ids = categories_df.loc[(categories_df['name'].isin(input.dropdown2())) & (categories_df['parent_id'].isin(top_level_id_list()))]
        level_2_id_list.set(level_2_ids['id'].values.tolist())
    
    ### create third dropdown ######
    @output
    @render.ui
    @reactive.event(level_2_id_list)
    def dropdown_level_3():
        #level_2_ids = categories_df[categories_df["id"].isin(level_2_id_list())]
        #level_2_ids = level_2_ids['id'].values.tolist()
        level_3_categories = categories_df[categories_df['parent_id'].isin(level_2_id_list())]

        return(ui.input_selectize('dropdown3', "Select 3rd level categories", level_3_categories['name'].values.tolist(),multiple=True))
    

    ## store IDs selected in 3rd dropdown #####

    level_3_id_list = reactive.Value([])

    ## server logic to get selected 3rd dropdown category IDs 
    @reactive.Effect
    @reactive.event(input.dropdown3,level_2_id_list)
    def add_level_3_ids():
        level_3_ids = categories_df.loc[(categories_df['name'].isin(input.dropdown3())) & (categories_df["parent_id"].isin(level_2_id_list()))]
        level_3_id_list.set(level_3_ids['id'].values.tolist())

    ### reactive list to store all category IDs from all dropdowns ####
    ## which will be used to query FRED API in analysis portion #####
    series_search_list = reactive.Value([])

    ### We don't want every single ID from all three dropdowns ####
    ### Instead, if a subcategory is selected, we want to throw out ###
    ### it's parent idea.  In contrast, if there is no subcategory selected ##
    ### we want to save that category ID, even if there are IDs from the ####
    ### dropdowns below ######################################################

    @reactive.Effect
    @reactive.event(level_3_id_list,level_2_id_list,top_level_id_list)
    def create_series_search_list():
        lvl2_parent_id = categories_df[categories_df["id"].isin(level_2_id_list())]
        lvl2_parent_id = lvl2_parent_id['parent_id'].values.tolist()

        lvl3_parent_id = categories_df[categories_df["id"].isin(level_3_id_list())]
        lvl3_parent_id = lvl3_parent_id['parent_id'].values.tolist()
        
        
        series_list = top_level_id_list() + level_2_id_list() + level_3_id_list()
        print(series_list)
        series_list = set(series_list)
        print(series_list)
        series_list = series_list - set(lvl2_parent_id) - set(lvl3_parent_id)
        print(series_list)
        series_search_list.set(series_list)

    ### user notification in bottom right of screen to notify that ########
    ### the FRED API is being used to get Series IDs for the chosen ######
    ### categories #############################################3

    @reactive.Effect
    @reactive.event(input.begin_series_search)
    def _():
        ui.notification_show("Fetching series IDs from FRED.  This may take a while", duration=15)
        

    ### reactive value to store all Series IDs received from category search ########
    full_series_list = reactive.Value([])

    #### On button click, start querying the FRED API for ###############
    ### Series within the selected categories #########################
    @reactive.Effect
    @reactive.event(input.begin_series_search)
    def prep_series_data():
        from full_fred.fred import Fred
        from datetime import datetime
        import time

        fred = Fred('api_key.txt')
        fred.get_api_key_file()
        # make sure the values from any old searches are deleted
        full_series_list.set([])
        
        # descend the categories to lowest point, then get series in that
        # subcategory
        for i in series_search_list():
            print(i)
            level_below = fred.get_child_categories(i)
            try:
                level_below_list = [d['id'] for d in level_below['categories']]
            except:
                level_below_list = []

            print(level_below['categories'])

            
            if (len(level_below_list) == 0) or (level_below is None):
                print("No child categories for category ID ",i, "checking for series")
                series = fred.get_series_in_a_category(i,limit=1000)
                if len(series['seriess']) > 0:
                    print("There are series to add:")
                    try:
                        full_series_list.set(full_series_list() + series['seriess'])
                    except:
                        pass
                else:
                    pass
            else:
                for k in level_below_list:
                    print(k)
                    level_2 = fred.get_child_categories(k)
                    try:
                        level_2_list = [d['id'] for d in level_2['categories']]
                    except:
                        level_2_list = []
                    print(level_2['categories'])
                    if (len(level_2_list) == 0) or (level_2 is None):
                        print("series at level 3")
                        series = fred.get_series_in_a_category(k,limit=1000)
                        try:
                            full_series_list.set(full_series_list() + series['seriess'])
                        except:
                            pass
                        print("pausing for 5 seconds")
                        time.sleep(5)
                        print('Resumed at', time.ctime())
                    else:
                        for j in level_2_list:
                            level_3 = fred.get_child_categories(j)
                            try:
                                level_3_list = [d['id'] for d in level_3['categories']]
                            except:
                                level_3_list = []

                            if (len(level_3_list) == 0) or (level_3 is None):
                                print("series at level 4")
                                series = fred.get_series_in_a_category(j,limit = 1000)
                                try:
                                    full_series_list.set(full_series_list() + series['seriess'])
                                except:
                                    pass
                                print("pausing for 5 seconds")
                                time.sleep(5)
                                print('Resumed at', time.ctime())
                            else:
                                for l in level_3['categories']:
                                    level_4 = fred.get_child_categories(l)
                                    try:
                                        level_4_list = [d['id'] for d in level_4['categories']]
                                    except:
                                        level_4_list = []

                                    if (len(level_4_list) == 0) or (level_4 is None):
                                        print("series at level 5")
                                        series = fred.get_series_in_a_category(l,limit = 1000)
                                        try:
                                            full_series_list.set(full_series_list() + series['seriess'])
                                        except:
                                            pass
                                        print("pausing for 5 seconds")
                                        time.sleep(5)
                                        print('Resumed at', time.ctime())
                                    else:
                                        pass    


            
            print("Child categories or series for ",i, ' are gathered. Pausing for 5 seconds', time.ctime())
            time.sleep(5)
            print('Resumed at', time.ctime())
        full_series_list.set([pd.DataFrame(full_series_list()).drop_duplicates()])

    ### create output table of the full series #####
    ### this is used no matter the source of the series ####

    @output
    @render.data_frame
    @reactive.event(full_series_list)
    def series_table():
        #df = pd.DataFrame(full_series_list())
        return render.DataTable(
                full_series_list()[0],
                height=500,
                width="100%",
                filters = False
        )
    
    ### prepare download button to download series ID data ####
    
    @session.download(filename="series.csv")
    def series_download():
        req(full_series_list())
        #df = pd.DataFrame(full_series_list())
        yield full_series_list()[0].to_csv(index=False)
    
    ## create UI section around series output table #####
    @output
    @render.ui
    @reactive.event(full_series_list)
    def series_table_section():
        req(full_series_list())

        return ui.TagList(
                ui.tags.h4("Series Dataset"),
                ui.download_button("series_download", "Download Series CSV"),
                ui.output_data_frame("series_table")
        )
    
    ### user notification to show user analysis has started ##

    @reactive.Effect
    @reactive.event(input.analysis_begin)
    def _():
        ui.notification_show("Fetching data and running analysis.  This may take a while", duration=15)

    ### reactive value to store results 
    
    results_df = reactive.Value([])

    ## big code chunk containing analysis logic ####
    ## essentially: 1) loop through baseline files ###
    ##              2) loop through series IDs
    ##              3) for each series ID, fetch series data from FRED
    ##              4) calculate spearman and pearson correlation between 
    ##                  baseline and series data, at all lags
    ##              5) store results
    @reactive.Effect
    @reactive.event(input.analysis_begin)
    def core_analysis():
        req(baseline_df_list())
        req(full_series_list())

        from full_fred.fred import Fred
        from datetime import datetime
        import time
        import numpy as np
        from scipy.stats import pearsonr, spearmanr

        fred = Fred('api_key.txt')
        fred.get_api_key_file()
        import warnings


        output_dict = {}

        for key in baseline_df_list()[0]:
            print(key)
            base_data = baseline_df_list()[0][key]
            base_data['Date'] = pd.to_datetime(base_data['Date'],yearfirst=True).dt.date
            OBSERVATION_START = base_data['Date'].min()
            OBSERVATION_END = base_data['Date'].max()
            output_dict[key] = {}
            count = 0

            for code in full_series_list()[0][full_series_list()[0].columns[0]]:
                print(code)
                if input.pcnt_change != 0:
                    indicators = fred.get_series_df(code,observation_start = OBSERVATION_START, observation_end = OBSERVATION_END)
                    if indicators.shape[1] > 0:
                        indicators = indicators.drop(['realtime_start','realtime_end'], axis=1)
                        indicators['value'] = pd.to_numeric(indicators['value'],errors='coerce')
                        indicators['value'] = indicators['value'].pct_change(input.pct_change()).replace([np.inf, -np.inf], [0, 0]).fillna(0)*100
                    else:
                        pass
                else:
                    indicators = fred.get_series_df(code,observation_start = OBSERVATION_START, observation_end = OBSERVATION_END)
                    print(indicators.head())
                    if indicators.shape[1] > 0:
                        indicators = indicators.drop(['realtime_start','realtime_end'], axis=1)
                        indicators['value'] = pd.to_numeric(indicators['value'],errors='coerce')
                    else:
                        pass

                if base_data.shape[0] > indicators.shape[0]:
                    base_data = base_data[0:indicators.shape[0]]
                elif indicators.shape[0] > base_data.shape[0]:
                    indicators = indicators[0:base_data.shape[0]]
                else:
                    pass

                if base_data.shape[0] < 15:
                    print("Insufficent data in this range")
                else:
                    indicators['date'] = pd.to_datetime(indicators['date'],yearfirst=True).dt.date
                    joined_data = pd.merge(base_data,indicators,left_on = "Date",right_on = "date", how = "inner")

                    joined_rows = joined_data.shape[0]

                    if joined_rows < 15:
                        print("Insufficent data in this range")
                    else:
                        output_dict[key][code] = {}
                        output_dict[key][code]['lag'] = [0]

                        
                        try:
                            pcorr = pearsonr(joined_data.iloc[:,1],joined_data.iloc[:,3])
                            scorr = spearmanr(joined_data.iloc[:,1],joined_data.iloc[:,3])
                            print(pcorr)
                            print(scorr)
                            output_dict[key][code]['pearsoncorr'] = [pcorr[0]]
                            output_dict[key][code]['pearson_pval'] = [pcorr[1]]
                            output_dict[key][code]['spearmancorr'] = [scorr[0]]
                            output_dict[key][code]['spearman_pval'] = [scorr[1]]
                        except:
                            output_dict[key][code]['pearsoncorr'] = [0]
                            output_dict[key][code]['pearson_pval'] = [0]
                            output_dict[key][code]['spearmancorr'] = [0]
                            output_dict[key][code]['spearman_pval'] = [0]

                        for i in range(1,input.lag()+1):
                            output_dict[key][code]['lag'].append(i)

                            try:
                                pcorr = pearsonr(joined_data.iloc[i:joined_rows,1],joined_data.iloc[0:joined_rows-i,3])
                                scorr = spearmanr(joined_data.iloc[i:joined_rows,1],joined_data.iloc[0:joined_rows-i,3])
                                print(pcorr)
                                print(scorr)
                                output_dict[key][code]['pearsoncorr'].append(pcorr[0])
                                output_dict[key][code]['pearson_pval'].append(pcorr[1])
                                output_dict[key][code]['spearmancorr'].append(scorr[0])
                                output_dict[key][code]['spearman_pval'].append(scorr[1])

                            except:
                                output_dict[key][code]['pearsoncorr'].append(0)
                                output_dict[key][code]['pearson_pval'].append(0)
                                output_dict[key][code]['spearmancorr'].append(0)
                                output_dict[key][code]['spearman_pval'].append(0)

                base_data = baseline_df_list()[0][key]
                count += 1
                if count % 100 == 0:
                    print(count, 'data sets pulled, waiting 45 seconds to resume at', time.ctime())
                    time.sleep(45)
                    print('Resumed at', time.ctime())

        output_frame = pd.DataFrame()
        for key in output_dict:
            code_dict = output_dict[key]
            for code in code_dict:
                df = pd.DataFrame.from_dict(code_dict[code])

                row_count = df.shape[0]
                df['baseline_data'] = np.repeat(key, row_count)
                df['indicator_code'] = np.repeat(code,row_count)
                output_frame = pd.concat([output_frame,df])


        results_df.set([output_frame])

    ### prepare output results table ###   
    @output
    @render.data_frame
    @reactive.event(results_df,input.pearson_thresh, input.spearman_thresh)
    def results_table():
        df = results_df()[0]
        df = df[(df['pearsoncorr'].abs() >= input.pearson_thresh()) & (df['spearmancorr'].abs() >= input.spearman_thresh())]
        return render.DataTable(
                df,
                height=500,
                width="100%",
                filters = False
        )

    ## create UI components for results section ###
    @output
    @render.ui
    @reactive.event(results_df)
    def results_table_section():
        req(results_df())

        return ui.TagList(
                ui.tags.h4("Results Table"),
                ui.download_button("results_download", "Download Results"),
                ui.output_data_frame("results_table")
        )
    
    ## create download handler to download results data
    @session.download(filename="results.csv")
    def results_download():
        req(results_df())
        df = results_df()[0]
        df = df[(df['pearsoncorr'].abs() >= input.pearson_thresh()) & (df['spearmancorr'].abs() >= input.spearman_thresh())]
        yield df.to_csv(index=False)

    ## notification to tell user Web scrape has started #####
    @reactive.Effect
    @reactive.event(input.web_scrape_begin)
    def _():
        ui.notification_show("Scraping FRED URL.  This may take a while", duration=15)

    ## server logic to perform web scrape
    @reactive.Effect
    @reactive.event(input.web_scrape_begin)
    def web_scrape():
        
        import httplib2
        from bs4 import BeautifulSoup, SoupStrainer
        import re

        codes = []
        names = []
        print(type(codes))
        print(type(names))
        print(input.scrape_url())
        url = input.scrape_url()
        print("Scraping URL: " + url)
        http = httplib2.Http()
        status, response = http.request(url)
        soup = BeautifulSoup(response, "html.parser")
        max_pages = int(soup.find("a", title=re.compile("last page"))['href'].split('=')[-1])

        for i in range(max_pages):
            print("page " + str(i))
            http = httplib2.Http()
            status, response = http.request(url+'&pageID='+str(i))
            soup = BeautifulSoup(response, "html.parser")
            for link in soup.find_all("a", href=re.compile("series/")):
                codes.append(link['href'].split('/')[-1])
                names.append(link.text.strip())

        codes = pd.DataFrame(list(set(list(zip(codes, names)))), columns = ['Codes', 'Names'])
        codes = codes[codes['Names'] != ''].drop_duplicates(subset='Codes')
        print(codes)
        full_series_list.set([codes])

    ## server logic to handle uploaded series IDs for analysis
    @reactive.Effect
    @reactive.event(input.series_file)
    def upload_contents():
        df = pd.read_csv(input.series_file()[0]['datapath'])
        full_series_list.set([df])


app = App(app_ui, server)
