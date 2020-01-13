package simpledb.parse;
import java.util.Scanner;

public class PredParserTest {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
      System.out.print("Enter an SQL predicate: ");
		while (sc.hasNext()) {
			String s = sc.nextLine();
			PredParser p = new PredParser(s);
			try {
				p.predicate();
				System.out.println("yes");
			}
			catch (BadSyntaxException ex) {
				System.out.println("no");
			}
         System.out.print("Enter an SQL predicate: ");
		}
		sc.close();
	}
}

class PredParser {
	private Lexer lex;

	public PredParser(String s) {
		lex = new Lexer(s);
	}

	public String field() {
		return lex.eatId();
	}

	public void constant() {
		if (lex.matchStringConstant())
			lex.eatStringConstant();
		else
			lex.eatIntConstant();
	}

	public void expression() {
		if (lex.matchId())
			field();
		else 
			constant();
	}

	public void term() {
		expression();
		lex.eatDelim('=');
		expression();
	}

	public void predicate() {
		term();
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			predicate();
		}
	}
}


