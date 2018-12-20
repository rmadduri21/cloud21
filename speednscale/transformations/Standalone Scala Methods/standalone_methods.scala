// start scala CLI with joda time in the classpath
scala -classpath Downloads/joda-time-2.7/joda-time-2.7.jar
import org.joda.time._
import org.joda.time.DateTime._
import org.joda.time.format._
import org.joda.time.convert._
import org.joda.time.DateTimeUtils._
import org.joda.time.DateTimeField._
import org.joda.time.convert.DateConverter


//DATE Functions using Joda Date Time Library




//This method acceps a date and returns date after a month

def oneMonthLater (date_in: String) = 
{ 
val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
val isoDate = fmt.parseDateTime(date_in)
val oneMonthLater = isoDate.plusMonths(1)
val printDt = println(oneMonthLater.toString("yyyy-MM-dd"))
    }

//Run like -
oneMonthLater("2014-01-01")

//Difference between two dates. Date 1 should be lower than Date 2

def dateDiff (date_1: String, date_2: String) : String = 
{ 
val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
val isoDate1 = fmt.parseDateTime(date_1)
val isoDate2 = fmt.parseDateTime(date_2)
val diff =  new Period(isoDate1, isoDate2)
// create a period formatter like this ...
val dys = new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways().appendYears().appendSuffix(" year ", " years ").appendDays().appendSuffix(" month ", " months ").appendDays().appendSuffix(" day", " days").toFormatter
// use the formatter to print the calculated period
val stringDate = dys print diff
return stringDate
    }

//Run the Diff like this 
dateDiff("2014-01-15","2015-11-20")

//The output will be like this 


//Convert yyyymmdd format to yyyy-mm-dd

def dateFormat (date_in: String) = 
{ 
val fmt = DateTimeFormat.forPattern("yyyyMMdd")
val isoDate = fmt.parseDateTime(date_in)
val printDt = println(isoDate.toString("yyyy-MM-dd"))
    }

Run like this -
dateFormat("20140101")

//Increment or Decrement date by number of months where number of months is parameter. Value can be negative months as well.

def calcMonths (date_in: String, num: Int) : String = 
{ 
val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
val isoDate = fmt.parseDateTime(date_in)
val calculateMonths = isoDate.plusMonths(num) 
val printDt = calculateMonths.toString("yyyy-MM-dd")
return printDt
    }

//Run like this
calcMonths("2015-01-01",4)

//Compare two dates and return integer indicating result 

def compDate (date_1: String, date_2: String): Int = 
{ 
//create a comparator
val dComp = DateTimeComparator.getDateOnlyInstance()

val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
val isoDate1 = fmt.parseDateTime(date_1)
val isoDate2 = fmt.parseDateTime(date_2)
val dCompare = dComp.compare(isoDate1, isoDate2)
return dCompare
    }

//Run like this -
compDate("2014-01-15","2015-11-20")


//String Functions

//Create MD5 for a given string

def md5Hash(text: String) : String =java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {"%02x".format(_) }.foldLeft(""){_ + _}

Run like this -
md5Hash("Hash this Text!")

//Split string on a given character

def splitString(text: String, delim: String): Array[String] = {

val arr = text.split(delim)
return arr
}

//Run like this -
splitString("convert_this_to_array_using_underscroe","_")


// Split string based at given substring location

def locSplit( indices : List[Int], s : String) = (indices zip (indices.tail)) map { case (a,b) => s.substring(a,b) }

//Run like this -

locSplit(List(0,4,6,8),"20150101")
//res0: List[String] = List(2015, 01, 01)


// Convert string to integer and return only when succeed. That is return null when cannot convert so that it doesn't fail

def toInt(in: String): Option[Int] = {
    try {
        Some(Integer.parseInt(in.trim))
    } catch {
        case e: NumberFormatException => None
    }
}


// Interchange Fee Calculator (BCUS use case)
/*
Hive UDF logic
.if (pi_Debit_Credit_Ind.toUpperCase().equals("C".toUpperCase()))
                       l_nAmt_Transaction = pi_Amt_Transaction.get() * -1;
               else
                       l_nAmt_Transaction = pi_Amt_Transaction.get();
              
               ln_interchg_Pct_rate_calc = nvl(pi_interchg_Percentage_Rate)/100;
              
               if (ln_interchg_Pct_rate_calc > 0 && nvl(pi_Interchg_PercentItm_Rate)> 0)
                       return (l_nAmt_Transaction * ln_interchg_Pct_rate_calc) + pi_Interchg_PercentItm_Rate.get();
               else if( ln_interchg_Pct_rate_calc > 0 && nvl(pi_Interchg_PercentItm_Rate) == 0)
                       return (l_nAmt_Transaction * ln_interchg_Pct_rate_calc);
               else if (ln_interchg_Pct_rate_calc ==0 && nvl(pi_Interchg_PercentItm_Rate)> 0)
                       return pi_Interchg_PercentItm_Rate.get();
               else
               return 0 ; */


def interchangeFeeCalc (pi_Debit_Credit_Ind: String, pi_Amt_Transaction: Float, pi_interchg_Percentage_Rate: Float, pi_Interchg_PercentItm_Rate: Float): Float =

{
val l_nAmt_Transaction = if (pi_Debit_Credit_Ind=='C') pi_Amt_Transaction * -1 else pi_Amt_Transaction
val ln_interchg_Pct_rate_calc = pi_interchg_Percentage_Rate/100
var ln_interchg_calc = 0.0f 
if (ln_interchg_Pct_rate_calc > 0 && pi_Interchg_PercentItm_Rate >0) 
    ln_interchg_calc = (l_nAmt_Transaction*ln_interchg_Pct_rate_calc)+pi_Interchg_PercentItm_Rate
if (ln_interchg_Pct_rate_calc > 0 && pi_Interchg_PercentItm_Rate==0)
    ln_interchg_calc = l_nAmt_Transaction * ln_interchg_Pct_rate_calc
if (ln_interchg_Pct_rate_calc==0 && pi_Interchg_PercentItm_Rate>0)
    ln_interchg_calc = pi_Interchg_PercentItm_Rate

return ln_interchg_calc
}




