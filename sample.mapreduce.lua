local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')

local data = {}
data[1] = "Bacon ipsum dolor amet strip steak pig t-bone rump meatball cupim pork loin boudin kevin alcatra frankfurter kielbasa short ribs jerky. Leberkas tri-tip cow pork chop cupim short ribs meatloaf, ground round corned beef tail boudin jowl rump picanha. Hamburger biltong sirloin capicola shankle pork loin, ribeye landjaeger. Short loin pork chop jerky, shank meatball leberkas tongue chuck tail doner cupim."
data[2] = "Drumstick strip steak shank ground round chicken. Sausage short ribs ground round, pork loin cupim kielbasa jowl porchetta prosciutto. Sirloin swine chicken, prosciutto tail capicola rump chuck alcatra filet mignon corned beef ground round pork. Chicken ham hock ball tip t-bone cupim beef."
data[3] = "Ham hock pork belly landjaeger, jerky beef ribs drumstick pork meatloaf meatball porchetta cow boudin. Flank bacon venison, porchetta alcatra prosciutto short loin chuck cupim tongue shank fatback t-bone doner leberkas. Picanha cupim t-bone leberkas, beef boudin kielbasa bresaola ribeye biltong tri-tip chicken. Pig tri-tip leberkas landjaeger cow pork."
data[4] = "Porchetta meatball beef ribs spare ribs tongue. Meatloaf cupim alcatra brisket prosciutto porchetta corned beef. Picanha frankfurter ground round, rump alcatra cupim turducken shank pork loin porchetta. Pork belly salami frankfurter sirloin prosciutto strip steak shank fatback beef ribs drumstick."
data[5] = "Cupim flank tri-tip tongue brisket. Beef ribs picanha boudin beef meatloaf turducken meatball shoulder venison chuck pork doner. Tenderloin tri-tip ground round fatback ham hock pork loin, leberkas sausage landjaeger tongue pork belly meatball. Filet mignon venison strip steak pastrami pork capicola landjaeger frankfurter boudin. Meatloaf biltong tail pastrami kielbasa, landjaeger ground round. Doner leberkas tri-tip, ham swine biltong picanha alcatra."

mapreduce.setup(
    "http://backspace.webscript.io/map",
    "http://backspace.webscript.io/reduce",
    "http://backspace.webscript.io/result")
mapreduce.map(data)
