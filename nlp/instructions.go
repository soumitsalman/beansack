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

	_RETRY_INSTRUCTION = "Format the input content in JSON format"
)

var (
	_DIGEST_SAMPLE_OUTPUT = Digest{
		Summary: "Large Language Models (LLMs) are explained in simple terms, revealing how they work under the hood as prediction machines that process tokens and enable human-seeming communication.",
		Topic:   "Large Language Models",
	}

	_CONCEPTS_SAMPLE_OUTPUT = []KeyConcept{
		{
			KeyPhrase:   "IEEE 802.11 Wi-Fi standard",
			Event:       "Design flaw in IEEE 802.11 Wi-Fi standard",
			Description: "Researchers at KU Leuven discovered a fundamental design flaw in the IEEE 802.11 Wi-Fi standard that gives attackers a way to trick victims into connecting with a less secure wireless network than the one to which they intended to connect.",
		},
		{
			KeyPhrase:   "D-Link DIR-X4860 routers",
			Event:       "Zero-day Exploit in D-Link DIR-X4860 routers",
			Description: "Researchers have released an exploit for a zero-day security flaw in D-Link DIR-X4860 routers, allowing attackers to take over devices and execute commands with root privileges.",
		},
		{
			KeyPhrase:   "Cinterion cellular modems",
			Event:       "Multiple security flaws in Cinterion cellular modems",
			Description: "Cybersecurity researchers have disclosed multiple security flaws in Cinterion cellular modems that could be potentially exploited by threat actors to access sensitive information and achieve code execution.",
		},
	}
)
