import pandas as pd
from pytrends.request import TrendReq
import time
import random

# 1. 完整明星名单整理 (去重处理)
raw_list = [
    "John O'Hurley", "Kelly Monaco", "Evander Holyfield", "Rachel Hunter", "Joey McIntyre", "Trista Sutter", 
    "Tatum O'Neal", "Tia Carrere", "George Hamilton", "Lisa Rinna", "Stacy Keibler", "Jerry Rice", 
    "Giselle Fernandez", "Master P", "Drew Lachey", "Kenny Mayne", "Harry Hamlin", "Vivica A. Fox", 
    "Monique Coleman", "Joey Lawrence", "Mario Lopez", "Emmitt Smith", "Shanna Moakler", "Willa Ford", 
    "Sara Evans", "Jerry Springer", "Tucker Carlson", "John Ratzenberger", "Ian Ziering", "Clyde Drexler", 
    "Laila Ali", "Apolo Anton Ohno", "Shandi Finnessey", "Paulina Porizkova", "Heather Mills", "Billy Ray Cyrus", 
    "Joey Fatone", "Leeza Gibbons", "Cameron Mathison", "Jane Seymour", "Sabrina Bryan", "Jennie Garth", 
    "Floyd Mayweather Jr.", "Josie Maran", "Albert Reed", "Helio Castroneves", "Mel B", "Wayne Newton", 
    "Marie Osmond", "Mark Cuban", "Cristian de la Fuente", "Steve Guttenberg", "Priscilla Presley", 
    "Marlee Matlin", "Shannon Elizabeth", "Marissa Jaret Winokur", "Jason Taylor", "Kristi Yamaguchi", 
    "Monica Seles", "Penn Jillette", "Adam Carolla", "Mario", "Ted McGinley", "Cloris Leachman", 
    "Susan Lucci", "Cody Linley", "Misty May-Treanor", "Maurice Greene", "Warren Sapp", "Jeffrey Ross", 
    "Toni Braxton", "Lance Bass", "Kim Kardashian", "Rocco DiSpirito", "Brooke Burke", "Gilles Marini", 
    "Denise Richards", "David Alan Grier", "Lawrence Taylor", "Ty Murray", "Shawn Johnson", "Steve Wozniak", 
    "Belinda Carlisle", "Chuck Wicks", "Lil' Kim", "Steve-O", "Holly Madison", "Melissa Rycroft", 
    "Ashley Hamiliton", "Debi Mazar", "Melissa Joan Hart", "Mark Dacascos", "Chuck Liddell", "Natalie Coughlin", 
    "Louie Vito", "Michael Irvin", "Joanna Krupa", "Kathy Ireland", "Tom DeLay", "Macy Gray", "Aaron Carter", 
    "Mya", "Donny Osmond", "Kelly Osbourne", "Aiden Turner", "Shannen Doherty", "Niecy Nash", "Buzz Aldrin", 
    "Chad Ochocinco", "Evan Lysacek", "Pamela Anderson", "Nicole Scherzinger", "Erin Andrews", "Kate Gosselin", 
    "Jake Pavelka", "David Hasselhoff", "Florence Henderson", "Kyle Massey", "Jennifer Grey", "Rick Fox", 
    "Kurt Warner", "Margaret Cho", "Michael Bolton", "Brandy", "The Situation", "Audrina Patridge", 
    "Bristol Palin", "Ralph Macchio", "Chelsea Kane", "Kirstie Alley", "Hines Ward", "Sugar Ray Leonard", 
    "Chris Jericho", "Petra Nemcova", "Mike Catherwood", "Romeo", "Wendy Williams", "Kendra Wilkinson", 
    "Elisabetta Canalis", "David Arquette", "Ricki Lake", "J.R. Martinez", "Metta World Peace", "Hope Solo", 
    "Chynna Phillips", "Kristin Cavallari", "Carson Kressley", "Chaz Bono", "Nancy Grace", "Rob Kardashian", 
    "William Levy", "Jack Wagner", "Sherri Sheperd", "Jaleel White", "Roshon Fegan", "Melissa Gilbert", 
    "Martina Navratilova", "Donald Driver", "Gavin DeGraw", "Gladys Knight", "Katherine Jenkins", "Maria Menounos", 
    "Zendaya", "Dorothy Hamill", "Victor Ortiz", "Alexandra Raisman", "Jacoby Jones", "D. L. Hughley", 
    "Andy Dick", "Wynonna Judd", "Kellie Pickler", "Lisa Vanderpump", "Sean Lowe", "Valerie Harper", 
    "Brant Daugherty", "Elizabeth Berkley Lauren", "Leah Remini", "Corbin Bleu", "Amber Riley", "Keyshawn Johnson", 
    "Bill Engvall", "Christina Milian", "Nicole \"Snooki\" Polizzi", "Jack Osbourne", "Bill Nye", 
    "Billy Dee Williams", "Danica McKellar", "James Maslow", "Candace Cameron Bure", "Sean Avery", "Diana Nyad", 
    "Charlie White", "Amy Purdy", "Meryl Davis", "Drew Carey", "Cody Simpson", "NeNe Leakes", "Antonio Sabato Jr.", 
    "Jonathan Bennett", "Lea Thompson", "Janel Parrish", "Alfonso Ribeiro", "Lolo Jones", "Randy Couture", 
    "Tommy Chong", "Betsey Johnson", "Michael Waltrip", "Tavis Smiley", "Bethany Mota", "Sadie Robertson", 
    "Suzanne Somers", "Willow Shields", "Riker Lynch", "Rumer Willis", "Nastia Liukin", "Michael Sam", 
    "Robert Herjavec", "Charlotte McKinney", "Noah Galloway", "Redfoo", "Patti LaBelle", "Chris Soules", 
    "Gary Busey", "Alexa PenaVega", "Carlos PenaVega", "Victor Espinoza", "Alek Skarlatos", "Chaka Khan", 
    "Andy Grammer", "Tamar Braxton", "Nick Carter", "Hayes Grier", "Bindi Irwin", "Kim Zolciak-Biermann", 
    "Paula Deen", "Mischa Barton", "Kim Fields", "Jodie Sweetin", "Doug Flutie", "Von Miller", "Antonio Brown", 
    "Paige VanZant", "Geraldo Rivera", "Nyle DiMarco", "Ginger Zee", "Wanya Morris", "Marla Maples", 
    "Jake T. Austin", "Maureen McCormick", "Marilu Henner", "Ryan Lochte", "Calvin Johnson Jr.", "Laurie Hernandez", 
    "Amber Rose", "Rick Perry", "James Hinchcliffe", "Babyface", "Vanilla Ice", "Jana Kramer", "Terra Jole", 
    "Charlo", "Mr. T", "Heather Morris", "Nancy Kerrigan", "Bonner Bolton", "Simone Biles", "David Ross", 
    "Rashad Jennings", "Chris Kattan", "Erika Jayne", "Normani", "Nick Viall", "Sasha Pieterse", "Frankie Muniz", 
    "Jordan Fisher", "Derek Fisher", "Nikki Bella", "Terrell Owens", "Victoria Arlen", "Barbara Corcoran", 
    "Lindsey Stirling", "Debbie Gibson", "Nick Lachey", "Drew Scott", "Vanessa Lachey", "Jamie Anderson", 
    "Johnny Damon", "Kareem Abdul-Jabbar", "Arike Ogunbowale", "Jennie Finch Daigle", "Chris Mazdzer", 
    "Mirai Nagasu", "Tonya Harding", "Josh Norman", "Adam Rippon", "Juan Pablo Di Pace", "Evanna Lynch", 
    "Nancy McKeon", "John Schneider", "Milo Manheim", "Danelle Umstead", "Mary Lou Retton", "DeMarcus Ware", 
    "Nikki Glaser", "Bobby Bones", "Tinashe", "Alexis Ren", "Joe Amabile", "Kate Flannery", "James Van Der Beek", 
    "Kel Mitchell", "Ray Lewis", "Lamar Odom", "Sailor Brinkley-Cook", "Sean Spicer", "Mary Wilson", 
    "Lauren Alaina", "Ally Brooke", "Karamo Brown", "Hannah Brown", "Anne Heche", "Jesse Metcalfe", 
    "Chrishell Stause", "Skai Jackson", "Justina Machado", "Charles Oakley", "Veron Davis", "Johnny Weir", 
    "Nev Schulman", "AJ McLean", "Nelly", "Kaitlyn Bristowe", "Carole Baskin", "Monica Aldama", "Jeannie Mai", 
    "Martin Kove", "Brian Austin Green", "Melora Hardin", "Matt James", "Mike \"The Miz\" Mizanin", "Suni Lee", 
    "Iman Shumpert", "Cody Rigsby", "Melanie C", "Jimmie Allen", "Olivia Jade", "JoJo Siwa", "Christine Chiu", 
    "Kenya Moore", "Amanda Kloots", "Jason Lewis", "Cheryl Ladd", "Selma Blair", "Joseph Baena", "Trevor Donovan", 
    "Daniel Durant", "Wayne Brady", "Sam Champion", "Jessie James Decker", "Jordin Sparks", "Heidi D'Amelio", 
    "Charli D'Amelio", "Teresa Giudice", "Vinny Guadagnino", "Shangela", "Gabby Windey", "Jamie Lynn Spears", 
    "Mira Sorvino", "Barry Williams", "Alyson Hannigan", "Xochitl Gomez", "Adrian Peterson", "Matt Walsh", 
    "Tyson Beckford", "Jason Mraz", "Lele Pons", "Harry Jowsey", "Mauricio Umansky", "Charity Lawson", 
    "Ariana Madix", "Tori Spelling", "Eric Roberts", "Reginald VelJohnson", "Chandler Kinney", "Dwight Howard", 
    "Danny Amendola", "Stephen Nedoroscik", "Ilona Maher", "Anna Delvey", "Brooks Nader", "Phaedra Parks", 
    "Jenn Tran", "Joey Graziadei", "Corey Feldman", "Danielle Fishel", "Elaine Hendrix", "Baron Davis", 
    "Jordan Chiles", "Andy Richter", "Robert Irwin", "Hilaria Baldwin", "Lauren Jauregui", "Scott Hoying", 
    "Alix Earle", "Jen Affleck", "Whitney Leavitt", "Dylan Efron"
]

# 明星去重
ALL_STARS = sorted(list(set(raw_list)))

# 2. 初始化 pytrends
pytrends = TrendReq(hl='en-US', tz=360)

def fetch_all_trends(star_list, output_file="dwts_historical_trends.csv"):
    """
    抓取 2004 至今的所有热度数据，通过 Long-term 趋势定位明星的 Popularity
    """
    all_data = pd.DataFrame()
    
    # Google Trends 限制一次 5 个关键词
    batch_size = 5
    
    for i in range(0, len(star_list), batch_size):
        batch = star_list[i:i + batch_size]
        print(f"正在处理第 {i//batch_size + 1} 组: {batch}")
        
        # 针对潜在歧义词的处理
        search_terms = []
        for name in batch:
            if name in ["Mario", "The Situation", "Prince", "Romeo"]:
                search_terms.append(f"{name} DWTS")
            else:
                search_terms.append(name)
        
        try:
            # 采用 'all' 时间范围 (2004-present) 以确保覆盖所有赛季
            pytrends.build_payload(search_terms, cat=0, timeframe='all', geo='US', gprop='')
            df = pytrends.interest_over_time()
            
            if not df.empty:
                df = df.drop(columns=['isPartial'])
                # 转为长表格式
                df_reset = df.reset_index()
                melted = df_reset.melt(id_vars='date', var_name='celebrity_name', value_name='search_index')
                all_data = pd.concat([all_data, melted])
                
            # 随机休眠防止封禁 (很重要！)
            wait = random.uniform(10, 20)
            time.sleep(wait)
            
        except Exception as e:
            print(f"抓取 {batch} 时发生错误: {e}")
            time.sleep(60) # 出错则长休
            continue
            
    # 保存结果
    all_data.to_csv(output_file, index=False)
    print(f"✅ 完成！数据已保存至 {output_file}")

# 3. 运行抓取
if __name__ == "__main__":
    fetch_all_trends(ALL_STARS)