package nlp

const (
	_DIGEST_INSTRUCTION = "You are provided with one documents delimitered by ```\n" +
		"For each user input you will extract the main digest of the document.\n" +
		"You MUST return exactly one digest.\n" +
		"A 'digest' contains a concise summary of the content and the content topic."
	_CONCEPTS_INSTRUCTION = "You are provided with one or more news article or social media post delimitered by ```\n" +
		"For each input you will extract the all the main keyconcepts from each document.\n" +
		"Each document can have more than one keyconcepts. Your output will be a list of keyconcepts.\n" +
		"A 'keyconcept' is one of the main messages or information that is central to the a news article, document or social media post.\n" +
		"A 'keyconcept' has a 'keyphrase' and an associated 'event' and 'description'."
	_RETRY_INSTRUCTION = "Format the INPUT content in JSON format"

	_DIGEST_SAMPLE_INPUT   = "You can never be sure what to expect out of Disney’s upfront presentation, but this year’s showcase of the studio’s new projects brought a slew of news about Disney Plus’ upcoming WandaVision spinoff series.While there’s been a bit of confusion about what the Agatha Harkness-focused series would ultimately be called, Kathryn Hahn, Patti Lupone, and Joe Locke revealed today that it will, in fact, be titled Agatha All Along, and its first two episodes will premiere on September 18th.A brief teaser for the series made it seem like Agatha All Along will find Harkness (Hahn) trapped in yet another show-within-a-show reality before a number of other witches free her, and it becomes clear that she’s lost most of her magical abilities. Compared to WandaVision, which had a playful sitcom tone, Agatha All Along looks like it’s going for a darker, more horror-oriented vibe. It’s not clear how the show is meant to fit into the larger MCU, but if it’s anything like its predecessor, it’s going to be a gas."
	_CONCEPTS_SAMPLE_INPUT = "Overdose deaths have surpassed 100,000 for the third straight year, according to federal data released Wednesday, a reminder that the nation remains mired in an intractable epidemic fueled by the potent street drug fentanyl.According to provisional data released by the Centers for Disease Control and Prevention, an estimated 107,543 people died in 2023, a slight decrease from the previous year. The agency described it as the first annual decrease in deaths since 2018, although experts cautioned that the numbers could rise in ensuing years and that the toll remains unacceptably high." +
		_BATCH_DELIMETER +
		"On Thursday evening, many iPhone owners (including some here at The Verge) saw the “not delivered” flag when trying to send texts via iMessage. People reported the problem across multiple wireless carriers (Verizon, AT&T, and T-Mobile), countries, and even continents.The Apple services status page didn’t show any indication of trouble while the problems were going on, but now it has been updated after the fact, reflecting a resolved issue where “Users were unable to use this service” for iMessage, Apple Messages for Business, FaceTime, and HomeKit. According to the note, the problems went on from about 5:39PM ET until 6:35PM ET.Screenshot: Apple.comApple has not responded to inquiries or otherwise commented on the issue; however, judging by our use and reports on social media, everything seems to be up and running again. However, if your international friends are still saying, “Just use WhatsApp!” there isn’t really anything we can do about that.Update, May 16th: Noted the issue appears to be resolved." +
		_BATCH_DELIMETER +
		"Skip to content\n\n    \n        \n\n        \n            \n\n    \n\n    \n        Pump It Up is a popular music video game that hails from South Korea. It’s similar in vibe to Dance Dance Revolution and In The Groove, but it has an extra arrow panel to make life harder. [Rodrigo Alfonso] loved it so much, he ported it to the Game Boy Advance.\nThe port looks fantastic, with all the fast-moving arrows and lovely sprite-based graphics you could dream of. But more than that, [Rodrigo’s] port is very fully featured. It doesn’t rely on tracked or sampled music, instead using actual GSM audio files for the songs.\nIt can also accept input from a PS/2 keyboard, and you can even do multiplayer over the GBA’s Wireless Adapter. What’s even cooler is that some of the game’s neat features have been broken out into separate libraries so other developers can use them. If you need a Serial Port library for the GBA, or a way to read the SD card on flash carts, [Rodrigo] has put the code on GitHub.\nAs you might have guessed, this isn’t the first time [Rodrigo] has pushed the limits on what Nintendo’s 32-bit handheld can do."
)

var (
	_DIGEST_SAMPLE_OUTPUT = Digest{
		Summary: "Disney Plus announces new WandaVision spinoff series titled Agatha All Along, with Kathryn Hahn reprising her role as Agatha Harkness. The show will premiere on September 18th with a darker, horror-oriented tone.",
		Topic:   "New Disney Plus Series",
	}

	// _CONCEPTS_SAMPLE_OUTPUT = []KeyConcept{
	// 	{
	// 		KeyPhrase:   "IEEE 802.11 Wi-Fi standard",
	// 		Event:       "Design flaw in IEEE 802.11 Wi-Fi standard",
	// 		Description: "Researchers at KU Leuven discovered a fundamental design flaw in the IEEE 802.11 Wi-Fi standard that gives attackers a way to trick victims into connecting with a less secure wireless network than the one to which they intended to connect.",
	// 	},
	// 	{
	// 		KeyPhrase:   "D-Link DIR-X4860 routers",
	// 		Event:       "Zero-day Exploit in D-Link DIR-X4860 routers",
	// 		Description: "Researchers have released an exploit for a zero-day security flaw in D-Link DIR-X4860 routers, allowing attackers to take over devices and execute commands with root privileges.",
	// 	},
	// 	{
	// 		KeyPhrase:   "Cinterion cellular modems",
	// 		Event:       "Multiple security flaws in Cinterion cellular modems",
	// 		Description: "Cybersecurity researchers have disclosed multiple security flaws in Cinterion cellular modems that could be potentially exploited by threat actors to access sensitive information and achieve code execution.",
	// 	},
	// }

	_CONCEPTS_SAMPLE_OUTPUT = KeyConceptList{
		Items: []KeyConcept{
			{
				KeyPhrase:   "Fentanyl",
				Event:       "Fentanyl fueling an intractable epidemic",
				Description: "Fentanyl, a potent street drug, has been linked to an estimated 107,543 overdose deaths in 2023, according to the Centers for Disease Control and Prevention.",
			},
			{
				KeyPhrase:   "iPhone",
				Event:       "iPhone experiencing iMessage issues",
				Description: "iPhone owners experienced issues with iMessage, with some users unable to send texts via the service.",
			},
			{
				KeyPhrase:   "Rodrigo Alfonso",
				Event:       "Porting Pump It Up to the Game Boy Advance",
				Description: "Rodrigo Alfonso ported the popular music video game Pump It Up to the Game Boy Advance, adding features such as PS/2 keyboard input and multiplayer over the GBA's Wireless Adapter.",
			},
		},
	}
)
