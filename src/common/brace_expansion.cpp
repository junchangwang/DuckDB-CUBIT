#include "duckdb/common/brace_expansion.hpp"

namespace duckdb {

// Not allow nested brace expansion and non-matching brace
bool BraceExpansion::has_brace_expansion(const string &pattern) {
	bool hasBrace = false;
	idx_t braceCount = 0;

	for (char ch : pattern) {
		// Detects nested braces
		if (ch == '{') {
			hasBrace = true;
			if (braceCount > 0) {
				break;
			}
			braceCount++;
			// Detects non-matching closing brace
		} else if (ch == '}') {
			if (braceCount <= 0) {
				break;
			}
			braceCount--;
		}
	}
	// Checks if all braces are matched and there is at least one brace
	if (braceCount == 0 && hasBrace) {
		return true;
	} else {
		// nested brace or non-matching brace
		if (braceCount > 0) {
			throw InvalidInputException("Not a vaild brace expansion file name");
		}
		// Without any brace
		return false;
	}
}

vector<string> BraceExpansion::brace_expansion(const string &pattern) {
	vector<std::string> result;
	idx_t firstBraceOpen = pattern.find('{');
	idx_t firstbraceClose = pattern.find('}');

	if (firstBraceOpen == string::npos || firstbraceClose == string::npos) {
		throw InvalidInputException("Cannot find brace during brace expansion");
	}

	string prefix = pattern.substr(0, firstBraceOpen);
	string suffix = pattern.substr(firstbraceClose + 1);
	string content = pattern.substr(firstBraceOpen + 1, firstbraceClose - firstBraceOpen - 1);
	std::stringstream contentStream(content);

	// dot dot expansion
	// check if the "start" and "end" is valid
	// we only support positive integer and single alphabet for dot dot expansion
	if (content.find("..") != string::npos) {
		idx_t dotdot = content.find("..");
		string start = content.substr(0, dotdot);
		string end = content.substr(dotdot + 2);

		// positive integer
		if (StringUtil::IsNaturalNumber(start) && StringUtil::IsNaturalNumber(end)) {
			idx_t s = std::stoi(start);
			idx_t e = std::stoi(end);
			if (s >= e) {
				throw InvalidInputException("Not a vaild brace expansion for dot dot expansion");
			}
			for (idx_t i = s; i <= e; ++i) {
				result.push_back(prefix + std::to_string(i) + suffix);
			}
		}
		// alphabet
		else if (start.size() == 1 && end.size() == 1) {
			char s = start[0];
			char e = end[0];
			if (StringUtil::CharacterIsAlpha(s) && StringUtil::CharacterIsAlpha(e)) {
				for (char c = s; c <= e; ++c) {
					result.push_back(prefix + std::string(1, c) + suffix);
				}
			}
		} else {
			throw InvalidInputException("Not a vaild brace expansion file name");
		}
		// comma expansion
	} else {
		string item;
		while (std::getline(contentStream, item, ',')) {
			result.push_back(prefix + item + suffix);
		}
	}

	// Handle muilple brace expansion recursively
	while (!result.empty() && has_brace_expansion(result.front())) {
		string pattern = result.front();
		result.erase(result.begin());
		vector<string> cartesian_product = brace_expansion(pattern);
		result.insert(result.end(), cartesian_product.begin(), cartesian_product.end());
	}

	return result;
}

} // namespace duckdb
